using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ServerSideCosmosDb
{
    public class StoredProceduresDemo
    {
        public static Uri MyStoreCollectionUri =>
            UriFactory.CreateDocumentCollectionUri("FamiliesDb", "mystore");
        public async static Task Run()
        {
            Debugger.Break();

            var endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
            var masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

            using (var client = new DocumentClient(new Uri(endpoint), masterKey))
            {
                //await CreateStoredProcedures(client);

                ViewStoredProcedures(client);

                await ExecuteStoredProcedures(client);

                //await DeleteStoredProcedures(client);
            }
        }

        private async static Task ExecuteStoredProcedures(DocumentClient client)
        {
            //await Execute_spHelloWorld(client);
            //await Execute_spSetNorthAmerica1(client);
            //await Execute_spSetNorthAmerica2(client);
            //await Execute_spSetNorthAmerica3(client);
            //await Execute_spEnsureUniqueId(client);
            await Execute_spBulkInsert(client);
            //await Execute_spBulkDelete(client);
        }

        private static async Task Execute_spBulkInsert(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spBulkInsert");

            var docs = new List<dynamic>();
            var total = 5000;
            for (var i = 1; i <= total; i++)
            {
                dynamic doc = new
                {
                    name = $"Bulk inserted doc {i}",
                    address = new
                    {
                        postalCode = "12345"
                    }
                };
                docs.Add(doc);
            }

            var uri = UriFactory.CreateStoredProcedureUri("FamiliesDb", "mystore", "spBulkInsert");
            var options = new RequestOptions { PartitionKey = new PartitionKey("12345") };

            var totalInserted = 0;
            while (totalInserted < total)
            {
                var result = await client.ExecuteStoredProcedureAsync<int>(uri, options, docs);
                var inserted = result.Response;
                totalInserted += inserted;
                var remaining = total - totalInserted;
                Console.WriteLine($"Inserted {inserted} documents ({totalInserted} total, {remaining} remaining)");
                docs = docs.GetRange(inserted, docs.Count - inserted);
            }
        }

        private async static Task Execute_spSetNorthAmerica1(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spSetNorthAmerica (country = United States)");

            // Should succeed with isNorthAmerica = true
            dynamic documentDefinition = new
            {
                name = "John Doe",
                address = new
                {
                    countryRegionName = "United States",
                    postalCode = "12345"
                }
            };
            var uri = UriFactory.CreateStoredProcedureUri("FamiliesDb", "mystore", "spSetNorthAmerica");
            var options = new RequestOptions { PartitionKey = new PartitionKey("12345") };
            var result = await client.ExecuteStoredProcedureAsync<object>(uri, options, documentDefinition, true);
            var document = result.Response;

            var id = document.id;
            var country = document.address.countryRegionName;
            var isNA = document.address.isNorthAmerica;

            Console.WriteLine("Result:");
            Console.WriteLine($" Id = {id}");
            Console.WriteLine($" Country = {country}");
            Console.WriteLine($" Is North America = {isNA}");

            string documentLink = document._self;
            await client.DeleteDocumentAsync(documentLink, options);
        }

        private async static Task Execute_spHelloWorld(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spHelloWorld stored procedure");

            var uri = UriFactory.CreateStoredProcedureUri("FamiliesDb", "mystore", "spHelloWorld");
            var options = new RequestOptions { PartitionKey = new PartitionKey(string.Empty) };
            var result = await client.ExecuteStoredProcedureAsync<string>(uri, options);
            var message = result.Response;

            Console.WriteLine($"Result: {message}");
        }

        private static void ViewStoredProcedures(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> View Stored Procedures <<<");
            Console.WriteLine();

            var sprocs = client
                .CreateStoredProcedureQuery(MyStoreCollectionUri)
                .ToList();

            foreach (var sproc in sprocs)
            {
                Console.WriteLine($"Stored procedure {sproc.Id}; RID: {sproc.ResourceId}");
            }
        }

        private async static Task CreateStoredProcedures(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Create Stored Procedures <<<");
            Console.WriteLine();

            await CreateStoredProcedure(client, "spHelloWorld");
            await CreateStoredProcedure(client, "spSetNorthAmerica");
            await CreateStoredProcedure(client, "spEnsureUniqueId");
            await CreateStoredProcedure(client, "spBulkInsert");
            await CreateStoredProcedure(client, "spBulkDelete");
        }

        private async static Task<StoredProcedure> CreateStoredProcedure(DocumentClient client, string sprocId)
        {
            var sprocBody = File.ReadAllText($@"..\..\Server\{sprocId}.js");

            var sprocDefinition = new StoredProcedure
            {
                Id = sprocId,
                Body = sprocBody
            };

            var result = await client.CreateStoredProcedureAsync(MyStoreCollectionUri, sprocDefinition);
            var sproc = result.Resource;
            Console.WriteLine($"Created stored procedure {sproc.Id}; RID: {sproc.ResourceId}");

            return result;
        }
    }
}
