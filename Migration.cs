using DbUp;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Helpers;
using DbUp.ScriptProviders;
using DbUpgrade.Models;
using DbUpgrade.Util;
using DbUpgrade.Util.Expression;
using Microsoft.Extensions.Options;
using Serilog;
using System.Text;
using Environment = DbUpgrade.Models.Environment;

namespace DbUpgrade
{
    internal partial class Migration(IOptions<AppConfig> config, HospitalIDProcessor hospitalIDProcessor, ReplaceLineEndingsProcessor replaceLineEndingsProcessor)
    {
        private readonly ILogger logger = Log.Logger;
        /// <summary>
        /// 已构建的引擎，key为数据库标识，value为升级引擎
        /// </summary>
        private readonly Dictionary<string, UpgradeEngine> _upgradeEngines = [];
        /// <summary>
        /// 分割符，用于分割脚本文件名中的医院
        /// </summary>
        private static readonly char[] separator = [',', '，'];
        /// <summary>
        /// 本次更新的版本号
        /// </summary>
        private static string VERSION = "";
        /// <summary>
        /// 脚本文件所在目录
        /// </summary>
        private string FILES_PATH = "";
        /// <summary>
        /// 已选中的目录、文件路径
        /// </summary>
        private string[] SELECTED_FILE_SYSTEMS = [];


        /// <summary>
        /// 执行升级
        /// </summary>
        /// <returns></returns>
        public void Migrate(string version, string filesPath, string[] selectFileSystems)
        {
            VERSION = version;
            FILES_PATH = filesPath;
            SELECTED_FILE_SYSTEMS = selectFileSystems;
            ConsoleUtil.WriteLine(
                $"当前环境：{config.Value.Environment}；\n" +
                $"当前医院：{config.Value.HospitalID}\n" +
                $"当前脚本所在目录：{filesPath}\n" +
                $"当前选择的目录或文件：{string.Join("\n", selectFileSystems)}\n" +
                $"本次更新版本号：{version}\n"
                , ConsoleColor.DarkYellow);
            BuildEngines();
            if (_upgradeEngines.Count == 0)
            {
                ConsoleUtil.WriteLine("没有可升级的数据库，脚本可能未放入或已执行并记录，请查看SchemaVersion表确认", ConsoleColor.Red);
                return;
            }
            PerformUpgrade();
        }

        #region 私有方法
        /// <summary>
        /// 构建升级引擎
        /// </summary>
        private void BuildEngines()
        {
            Console.WriteLine("初始化中，请稍后。。。");

            // 读取所有现有的连接
            var conns = config.Value.ConnectionStrings
            .Where(m => !string.IsNullOrEmpty(m.Value))
            .ToDictionary(m => m.Key, m => m.Value);
            if (conns.Count == 0)
            {
                throw new Exception("没有可连接的数据库！请检查ConnectionStrings配置是否正常");
            }
            foreach (var conn in conns)
            {
                var engine = BuildEngine(conn);
                // 根据版控配置和开发环境配置确定执行脚本中是否包含版本控制脚本
                var scriptNames = engine.GetScriptsToExecute();
                if (scriptNames.Count > 0)
                {
                    logger.Information($"数据库{conn.Key}待执行脚本：\n{string.Join("\n", scriptNames.Select(m => m.Name))}", ConsoleColor.DarkYellow);
                }
                // 若
                // ①数据库存在版控，但只有一个脚本且是版本号更新脚本
                // ②无论是否存在版控，数据库脚本为空
                // 则跳过当前数据库更新
                bool onlyVersionScript = config.Value.VersionDbs.Contains(conn.Key) && scriptNames.Count == 1 && scriptNames.First().Name.StartsWith('V');
                bool emptyScript = scriptNames.Count == 0;
                if (!onlyVersionScript && !emptyScript)
                {
                    HtmlReportBuilder.Build(engine, $"{conn.Key}-{DateTime.Now:yyyyMMddHHmmssffff}", VERSION);
                    _upgradeEngines.TryAdd(conn.Key, engine);
                }
            }
        }

        /// <summary>
        /// 构建一个升级引擎
        /// </summary>
        /// <param name="conn">连接字符串键值对</param>
        private UpgradeEngine BuildEngine(KeyValuePair<string, string> conn)
        {
            var isDev = config.Value.Environment == Environment.Development;
            config.Value.DbTypes.TryGetValue(conn.Key, out var dbType);

            var builder = DeployChanges.To
                .SetDb(dbType.ToString(), conn.Value)
                // 所有脚本均只执行一次，执行后不再重复执行
                .WithScriptsFromFileSystem(FILES_PATH, GetScriptOptions(config, conn.Key))
                .WithExecutionTimeout(TimeSpan.FromMinutes(300))
                .LogTo(new Logger(logger))
                // dbup官方所提供的变量替换功能，会导致\r被异常
                .WithVariablesDisabled()
                // 版本号更新脚本最后执行
                .WithVersionScript(config.Value.VersionDbs, conn.Key, VERSION, config.Value.Environment)
                .WithPreprocessor(replaceLineEndingsProcessor)
                .WithPreprocessor(hospitalIDProcessor)
                .WithTransactionPerScript();
            // 开发环境不记录执行日志 保证脚本重复执行
            var upgradeEngine = !isDev ? builder.Build() : builder.JournalTo(new NullJournal()).Build();
            var isConnected = upgradeEngine.TryConnect(out var errMsg);
            if (!isConnected)
            {
                throw new Exception($"连接数据库{conn.Key}失败！，原因：{errMsg}");
            }
            return upgradeEngine;
        }

        /// <summary>
        /// 获取文件系统选项
        /// </summary>
        /// <param name="config">配置</param>
        /// <param name="dbIdentifier">数据库标识</param>
        /// <returns></returns>
        private FileSystemScriptOptions GetScriptOptions(IOptions<AppConfig> config, string dbIdentifier)
        {
            var isProduction = config.Value.HospitalID != "0";
            #region 组装筛选条件表达式
            // 过滤选中的目录、文件
            bool filterSelectFileSystemOrFile(string filePath)
            {
                return SELECTED_FILE_SYSTEMS.Any(m => filePath == m || (Path.GetDirectoryName(filePath)! + @"\").StartsWith(m));
            }
            ;
            // 解析文件名，按规范过滤
            var fileFilter = PredicateBuilder.True<string>()
                // 生产环境需要判断第2部分的字符串，是否和AppSettings中一致，或为0，若都不是，则过滤掉此脚本
                .IfAnd(isProduction, script =>
                (script.Split("-", StringSplitOptions.None).ElementAtOrDefault(1) ?? "").Split(separator).Any(hospitalID => hospitalID == "0" || hospitalID == config.Value.HospitalID))
                // 根据数据库标识过滤出相应的脚本
                .And(script => (script.Split("-", StringSplitOptions.None).ElementAtOrDefault(2) ?? "").Equals(dbIdentifier.ToString(), StringComparison.CurrentCultureIgnoreCase))
                .Compile();
            #endregion

            var scriptOptions = new FileSystemScriptOptions
            {
                IncludeSubDirectories = true,
                Extensions = ["*.sql"],
                Encoding = Encoding.GetEncoding(936),
                UseOnlyFilenameForScriptName = false,
                // 排除特定文件，以及不符合要求的文件
                Filter = path => filterSelectFileSystemOrFile(path) && fileFilter(Path.GetFileName(path))
            };
            return scriptOptions;
        }

        /// <summary>
        /// 执行升级
        /// </summary>
        private void PerformUpgrade()
        {
            // 逐一执行
            for (var i = 0; i < _upgradeEngines.Count; i++)
            {
                var (key, upgradeEngine) = _upgradeEngines.ElementAt(i);
                logger.Information($"第{i + 1}个数据库{key}开始升级，预计将执行{upgradeEngine.GetScriptsToExecute().Count}个脚本", ConsoleColor.DarkCyan);

                var result = upgradeEngine.PerformUpgrade();
                if (!result.Successful)
                {
                    logger.Error($"数据库{key}升级失败", ConsoleColor.Red);
                    logger.Error(result.Error, $"执行脚本{result.ErrorScript.Name}失败！", ConsoleColor.Red);
                    return;
                }
            }
            logger.Information("所有数据库升级完成", ConsoleColor.DarkGreen);
        }
        #endregion
    }
}
