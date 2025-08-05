using DbUpgrade;
using DbUpgrade.Models;
using DbUpgrade.Util;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Serilog;
using System.Text;

// 检查是否有传递路径
if (args.Length == 0)
{
    ConsoleUtil.Error("未传路径或文件！请将一个或多个文件拖拽到此程序上以运行。", ConsoleColor.Red);
    return;
}

// 运行前，清除此前的html报告
if (Directory.Exists(AppContext.BaseDirectory + @"/htmlReport"))
{
    FileProcessor.DeleteDirectories([AppContext.BaseDirectory + @"/htmlReport"]);
}
// 注册编码
Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
using var host = Host.CreateDefaultBuilder(args)
    .UseSerilog()
    .ConfigureAppConfiguration((context, config) =>
    {
        config.SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
    })
    .ConfigureServices((context, services) =>
    {
        services.Configure<AppConfig>(context.Configuration);
        services.AddSingleton<ReplaceLineEndingsProcessor>();
        services.AddSingleton((provider) =>
        {
            var appConfig = provider.GetRequiredService<IOptions<AppConfig>>();
            return new HospitalIDProcessor
            {
                AppConfig = appConfig
            };
        });
        services.AddSingleton<Migration>();
        services.AddLogging(loggingBuilder => loggingBuilder.AddSerilog(dispose: true));
    }).Build();
await host.StartAsync();

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .WriteTo.File("logs/log-.txt", outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss}] {Level:u3} {Message:lj}{NewLine}{Exception}", rollingInterval: RollingInterval.Minute, retainedFileCountLimit: 7)
    .CreateLogger();
var filesPath = "";
var version = "";
var extractFileSystems = new List<string>();
var selectFileSystems = Array.Empty<string>();
// 如果传的本就是一个目录，执行这个目录下的所有文件，且尝试取目录名作为版本号
if (args.Length == 1 && Directory.Exists(args[0]))
{
    var directoryInfo = new DirectoryInfo(args[0]);
    version = RegexDeclare.Version().Match(directoryInfo.Name).Value;
    filesPath = args[0] + @"\";
    selectFileSystems = [args[0] + @"\"];
}
else
{
    var pathByGroup = args.GroupBy(Path.GetDirectoryName).ToList();
    if (pathByGroup.Count > 1)
    {
        ConsoleUtil.Error("只能选择同一目录下的目录或文件！当前选择：\n", ConsoleColor.Red);
        var pathStr = string.Join("\n", pathByGroup.Select(group => $"{group.Key}：\n{string.Join("\n", group.ToList())}\n"));
        ConsoleUtil.Error(pathStr, ConsoleColor.DarkRed);
    }

    filesPath = pathByGroup.First().Key!;
    (version, extractFileSystems) = FileProcessor.ExtractPackagesAndGetVersion(args);
    // 选中的目录、文件路径，需要加上已解压的目录
    selectFileSystems = args.Where(m => !FileProcessor.packageExtensions.Any(n => m.EndsWith(n))).Concat(extractFileSystems)
        // 如果是目录，需要再补个斜杠，以免后续过滤脚本时，多匹配到未选中的目录
        .Select(m => Directory.Exists(m) ? m + @"\" : m).ToArray();
}

Console.Clear();
var migration = host.Services.GetRequiredService<Migration>();
try
{
    migration.Migrate(version, filesPath, selectFileSystems);
}
catch (Exception e)
{
    Console.WriteLine(e.ToString());
}
FileProcessor.DeleteDirectories(extractFileSystems);
Console.WriteLine("按任意键退出...");
Console.ReadKey();