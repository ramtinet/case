using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Localization;
using Microsoft.Extensions.Logging;
using OrchardCore.Abstractions.Setup;
using OrchardCore.Data;
using OrchardCore.Environment.Shell;
using OrchardCore.Environment.Shell.Builders;
using OrchardCore.Environment.Shell.Descriptor;
using OrchardCore.Environment.Shell.Descriptor.Models;
using OrchardCore.Modules;
using OrchardCore.Recipes.Models;
using OrchardCore.Recipes.Services;
using OrchardCore.Setup.Events;
using YesSql;

namespace OrchardCore.Setup.Services;

/// <summary>
/// Represents a setup service.
/// </summary>
public class SetupService : ISetupService
{
    private readonly IShellHost _shellHost;
    private readonly IShellContextFactory _shellContextFactory;
    private readonly ISetupUserIdGenerator _setupUserIdGenerator;
    private readonly IEnumerable<IRecipeHarvester> _recipeHarvesters;
    private readonly ILogger _logger;
    protected readonly IStringLocalizer S;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly IDbConnectionValidator _dbConnectionValidator;
    private readonly string _applicationName;
    private IEnumerable<RecipeDescriptor> _recipes;

    /// <summary>
    /// Creates a new instance of <see cref="SetupService"/>.
    /// </summary>
    /// <param name="shellHost">The <see cref="IShellHost"/>.</param>
    /// <param name="hostingEnvironment">The <see cref="IHostEnvironment"/>.</param>
    /// <param name="shellContextFactory">The <see cref="IShellContextFactory"/>.</param>
    /// <param name="setupUserIdGenerator">The <see cref="ISetupUserIdGenerator"/>.</param>
    /// <param name="recipeHarvesters">A list of <see cref="IRecipeHarvester"/>s.</param>
    /// <param name="logger">The <see cref="ILogger"/>.</param>
    /// <param name="stringLocalizer">The <see cref="IStringLocalizer"/>.</param>
    /// <param name="applicationLifetime">The <see cref="IHostApplicationLifetime"/>.</param>
    /// <param name="httpContextAccessor">The <see cref="IHttpContextAccessor"/>.</param>
    /// <param name="dbConnectionValidator">The <see cref="IDbConnectionValidator"/>.</param>
    public SetupService(
        IShellHost shellHost,
        IHostEnvironment hostingEnvironment,
        IShellContextFactory shellContextFactory,
        ISetupUserIdGenerator setupUserIdGenerator,
        IEnumerable<IRecipeHarvester> recipeHarvesters,
        ILogger<SetupService> logger,
        IStringLocalizer<SetupService> stringLocalizer,
        IHostApplicationLifetime applicationLifetime,
        IHttpContextAccessor httpContextAccessor,
        IDbConnectionValidator dbConnectionValidator)
    {
        _shellHost = shellHost;
        _applicationName = hostingEnvironment.ApplicationName;
        _shellContextFactory = shellContextFactory;
        _setupUserIdGenerator = setupUserIdGenerator;
        _recipeHarvesters = recipeHarvesters;
        _logger = logger;
        S = stringLocalizer;
        _applicationLifetime = applicationLifetime;
        _httpContextAccessor = httpContextAccessor;
        _dbConnectionValidator = dbConnectionValidator;
    }

    /// <inheritdoc />
    public async Task<IEnumerable<RecipeDescriptor>> GetSetupRecipesAsync()
    {
        if (_recipes is null)
        {
            var recipeCollections = await Task.WhenAll(_recipeHarvesters.Select(x => x.HarvestRecipesAsync()));
            _recipes = recipeCollections.SelectMany(x => x).Where(x => x.IsSetupRecipe).ToArray();
        }

        return _recipes;
    }

    /// <inheritdoc />
    public async Task<string> SetupAsync(SetupContext context)
    {
        var initialState = context.ShellSettings.State;
        try
        {
            var executionId = await SetupInternalAsync(context);

            if (context.Errors.Count > 0)
            {
                context.ShellSettings.State = initialState;
                await _shellHost.ReloadShellContextAsync(context.ShellSettings, eventSource: false);
            }

            return executionId;
        }
        catch
        {
            context.ShellSettings.State = initialState;
            await _shellHost.ReloadShellContextAsync(context.ShellSettings, eventSource: false);

            throw;
        }
    }

    private async Task<string> SetupInternalAsync(SetupContext context)
    {
        context.LogObject.Content.Add("SetupInternalAsync", "SetupService.cs");
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Running setup for tenant '{TenantName}'.", context.ShellSettings?.Name);
            context.LogObject.Content.Add("Line 117, Running setup for tenant", context.ShellSettings?.Name);
        }

        // Features to enable for Setup.
        string[] coreFeatures =
        [
            _applicationName,
            "OrchardCore.Features",
            "OrchardCore.Scripting",
            "OrchardCore.Recipes"
        ];

        context.EnabledFeatures = coreFeatures.Union(context.EnabledFeatures ?? []).Distinct().ToList();

        // Set shell state to "Initializing" so that subsequent HTTP requests are responded to with "Service Unavailable" while Orchard is setting up.
        context.ShellSettings.AsInitializing();
        context.LogObject.Content.Add("Line 133, setting ShellSettings.settings.State to TenantState.Initializing", "Initializing");

        // Due to database collation we normalize the userId to lower invariant.
        // During setup there are no users so we do not need to check unicity.
        var adminUserId = _setupUserIdGenerator.GenerateUniqueId().ToLowerInvariant();
        context.LogObject.Content.Add("Line 138, Generating adminUserId", adminUserId);
        context.Properties[SetupConstants.AdminUserId] = adminUserId;

        var recipeEnvironmentFeature = new RecipeEnvironmentFeature();
        recipeEnvironmentFeature.Properties[SetupConstants.AdminUserId] = adminUserId;

        if (context.Properties.TryGetValue(SetupConstants.AdminUsername, out var adminUsername))
        {
            recipeEnvironmentFeature.Properties[SetupConstants.AdminUsername] = adminUsername;
        }

        if (context.Properties.TryGetValue(SetupConstants.SiteName, out var siteName))
        {
            recipeEnvironmentFeature.Properties[SetupConstants.SiteName] = siteName;
        }

        _httpContextAccessor.HttpContext.Features.Set(recipeEnvironmentFeature);

        var shellSettings = new ShellSettings(context.ShellSettings).ConfigureDatabaseTableOptions();
        context.LogObject.Content.Add("Line 158, Creating new ShellSettings from context.ShellSettings, and assinging tables to it", "ShellSettings");
        if (string.IsNullOrWhiteSpace(shellSettings["DatabaseProvider"]))
        {
            shellSettings["DatabaseProvider"] = context.Properties.TryGetValue(SetupConstants.DatabaseProvider, out var databaseProvider) ? databaseProvider?.ToString() : string.Empty;
            shellSettings["ConnectionString"] = context.Properties.TryGetValue(SetupConstants.DatabaseConnectionString, out var databaseConnectionString) ? databaseConnectionString?.ToString() : string.Empty;
            shellSettings["TablePrefix"] = context.Properties.TryGetValue(SetupConstants.DatabaseTablePrefix, out var databaseTablePrefix) ? databaseTablePrefix?.ToString() : string.Empty;
            shellSettings["Schema"] = context.Properties.TryGetValue(SetupConstants.DatabaseSchema, out var schema) ? schema?.ToString() : null;
            context.LogObject.Content.Add("Line 159, DatabaseProvider is present, we populate shellSettings with DatabaseProvider, ConnectionString, TablePrefix and Schema", "-");
        }

        if (shellSettings["DatabaseProvider"] == DatabaseProviderValue.Sqlite && string.IsNullOrEmpty(shellSettings["DatabaseName"]))
        {
            shellSettings["DatabaseName"] = context.Properties.TryGetValue(SetupConstants.DatabaseName, out var dbName) ? dbName?.ToString() : "OrchardCore.db";
            context.LogObject.Content.Add("Line 168, DatabaseProvider is Sqlite and no DatabaseName was provided, we set the DatabaseName to", shellSettings["DatabaseName"]);
        }

        var validationContext = new DbConnectionValidatorContext(shellSettings);
        context.LogObject.Content.Add("Line 174, new DbConnectionValidatorContext from shellSettings", "-");
        switch (await _dbConnectionValidator.ValidateAsync(validationContext))
        {
            case DbConnectionValidatorResult.NoProvider:
                context.Errors.Add(string.Empty, S["DatabaseProvider setting is required."]);
                break;
            case DbConnectionValidatorResult.UnsupportedProvider:
                context.Errors.Add(string.Empty, S["The provided database provider is not supported."]);
                break;
            case DbConnectionValidatorResult.InvalidConnection:
                context.Errors.Add(string.Empty, S["The provided connection string is invalid or server is unreachable."]);
                break;
            case DbConnectionValidatorResult.InvalidCertificate:
                context.Errors.Add(string.Empty, S["The security certificate on the server is from a non-trusted source (the certificate issuing authority isn't listed as a trusted authority in Trusted Root Certification Authorities on the client machine). In a development environment, you have the option to use the '{0}' parameter in your connection string to bypass the validation performed by the certificate authority.", "TrustServerCertificate=True"]);
                break;
            case DbConnectionValidatorResult.DocumentTableFound:
                context.Errors.Add(string.Empty, S["The provided database, table prefix and schema are already in use."]);
                break;
        }

        if (context.Errors.Count > 0)
        {
            return null;
        }

        // Creating a standalone environment based on a "minimum shell descriptor".
        // In theory this environment can be used to resolve any normal components by interface, and those
        // components will exist entirely in isolation - no crossover between the safemode container currently in effect
        // It is used to initialize the database before the recipe is run.
        var shellDescriptor = new ShellDescriptor
        {
            Features = context.EnabledFeatures.Select(id => new ShellFeature(id)).ToList()
        };
        context.LogObject.Content.Add("Line 204, new ShellDescriptor from ShellFeatures", "-");

        string executionId;

        await using (var shellContext = await _shellContextFactory.CreateDescribedContextAsync(shellSettings, shellDescriptor))
        {
            await (await shellContext.CreateScopeAsync()).UsingServiceScopeAsync(async scope =>
            {
                try
                {
                    // Create the "minimum" shell descriptor.
                    await scope.ServiceProvider.GetService<IShellDescriptorManager>()
                        .UpdateShellDescriptorAsync(0, shellContext.Blueprint.Descriptor.Features);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "An error occurred while initializing the datastore.");
                    context.Errors.Add(string.Empty, S["An error occurred while initializing the datastore: {0}", e.Message]);
                }
            });

            if (context.Errors.Count > 0)
            {
                return null;
            }

            executionId = Guid.NewGuid().ToString("n");
            context.LogObject.Content.Add("Line 234, new executionId", executionId);

            var recipeExecutor = shellContext.ServiceProvider.GetRequiredService<IRecipeExecutor>();
            try
            {
                await recipeExecutor.ExecuteAsync(executionId, context.Recipe, context.Properties, _applicationLifetime.ApplicationStopping);
                context.LogObject.Content.Add("Line 240, Executing recipe", context.Recipe.Author);
            }
            catch (RecipeExecutionException e)
            {
                _logger.LogError(e, "Unable to import a recipe during setup.");

                context.Errors.Add(string.Empty, string.Join(' ', e.StepResult.Errors));
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Unable to import a recipe during setup.");

                context.Errors.Add(string.Empty, S["Unexpected error occurred while importing the recipe."]);
            }
        }

        // Reloading the shell context as the recipe has probably updated its features.
        await (await _shellHost.GetScopeAsync(shellSettings)).UsingAsync(async scope =>
        {
            var handlers = scope.ServiceProvider.GetServices<ISetupEventHandler>();
            var logger = scope.ServiceProvider.GetRequiredService<ILogger<SetupService>>();

            await handlers.InvokeAsync((handler, ctx) => handler.SetupAsync(ctx), context, _logger);

            if (context.Errors.Count > 0)
            {
                await handlers.InvokeAsync((handler) => handler.FailedAsync(context), _logger);
            }
        });
        context.LogObject.Content.Add("Line 258, Reloaded shell context", "-");


        if (context.Errors.Count > 0)
        {
            return executionId;
        }

        // When using SQLite, clearing the connection pool with ReadWriteCreate SqliteOpenMode, used when the shell was
        // still initializing, to unlock the database file (and thus also allow it to be deleted).
        if (shellSettings["DatabaseProvider"] == DatabaseProviderValue.Sqlite)
        {
            await using var shellContext = await _shellContextFactory.CreateMinimumContextAsync(shellSettings);
            var store = shellContext.ServiceProvider.GetRequiredService<IStore>();
            await using var connection = store.Configuration.ConnectionFactory.CreateConnection();
            if (connection is SqliteConnection sqliteConnection)
            {
                SqliteConnection.ClearPool(sqliteConnection);
            }
            context.LogObject.Content.Add("Line 280, DatabaseProvider is Sqlite, we will close the sqllite connection", "-");
        }

        // Update the shell state.
        await _shellHost.UpdateShellSettingsAsync(shellSettings.AsRunning());
        context.LogObject.Content.Add("Line 293, Update the shell state to Running", "Running");
        await (await _shellHost.GetScopeAsync(shellSettings.Name)).UsingAsync(async scope =>
        {
            var handlers = scope.ServiceProvider.GetServices<ISetupEventHandler>();
            await handlers.InvokeAsync((handler) => handler.SucceededAsync(), _logger);
        });
        context.LogObject.Content.Add("Line 299, Setup succeeded", "-");
        context.LogObject.Content.Add("END", "SetupService.cs");
        return executionId;
    }
}
