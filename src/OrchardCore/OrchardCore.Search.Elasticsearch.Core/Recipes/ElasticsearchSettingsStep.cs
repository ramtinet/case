using OrchardCore.Recipes.Models;
using OrchardCore.Recipes.Services;
using OrchardCore.Search.Elasticsearch.Core.Services;

namespace OrchardCore.Search.Elasticsearch.Core.Recipes;

/// <summary>
/// This recipe step is used to sync Elasticsearch and Lucene settings.
/// </summary>
public sealed class ElasticsearchSettingsStep : NamedRecipeStepHandler
{
    private readonly ElasticsearchIndexingService _elasticIndexingService;

    public ElasticsearchSettingsStep(ElasticsearchIndexingService elasticIndexingService)
        : base("Settings")
    {
        _elasticIndexingService = elasticIndexingService;
    }

    protected override Task HandleAsync(RecipeExecutionContext context)
    {
        var step = context.Step["ElasticSettings"];

        if (step != null && step["SyncWithLucene"] != null && step["SyncWithLucene"].GetValue<bool>())
        {
            return _elasticIndexingService.SyncSettings();
        }

        return Task.CompletedTask;
    }
}