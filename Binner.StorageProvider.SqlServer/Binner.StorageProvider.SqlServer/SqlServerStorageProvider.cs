using Binner.Model.Common;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.SqlTypes;
using System.Linq.Expressions;
using TypeSupport.Extensions;
using static Binner.Model.Common.SystemDefaults;

namespace Binner.StorageProvider.SqlServer
{
    /// <summary>
    /// A storage provider for Sql Server
    /// </summary>
    public class SqlServerStorageProvider : IStorageProvider
    {
        public const string ProviderName = "SqlServer";

        private readonly SqlServerStorageConfiguration _config;
        private bool _isDisposed;
        private string _databaseName = "Binner";

        public SqlServerStorageProvider(IDictionary<string, string> config)
        {
            _config = new SqlServerStorageConfiguration(config);
            try
            {
                // run synchronously
                GenerateDatabaseIfNotExistsAsync<BinnerDbV5>()
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception ex)
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Failed to generate database! {ex.GetType().Name} = {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Get an instance of the entire database
        /// </summary>
        /// <returns></returns>
        public async Task<IBinnerDb> GetDatabaseAsync(IUserContext? userContext)
        {
            var parts = await GetPartsAsync();
            return new BinnerDbV5
            {
                OAuthCredentials = await GetOAuthCredentialAsync(userContext),
                Parts = parts,
                PartTypes = await GetPartTypesAsync(userContext),
                Projects = await GetProjectsAsync(userContext),
                StoredFiles = await GetStoredFilesAsync(userContext),
                OAuthRequests = await GetOAuthRequestsAsync(userContext),
                Pcbs = await GetPcbsAsync(userContext),
                PcbStoredFileAssignments = await GetPcbStoredFileAssignmentsAsync(userContext),
                ProjectPartAssignments = await GetProjectPartAssignmentsAsync(userContext),
                ProjectPcbAssignments = await GetProjectPcbAssignmentsAsync(userContext),
                PartSuppliers = await GetPartSuppliersAsync(userContext),
                Count = parts.Count,
                FirstPartId = parts.OrderBy(x => x.PartId).First().PartId,
                LastPartId = parts.OrderBy(x => x.PartId).Last().PartId,
            };
        }

        public async Task<ConnectionResponse> TestConnectionAsync()
        {
            try
            {
                using var connection = new SqlConnection(_config.ConnectionString);
                connection.Open();
                using var sqlCmd = new SqlCommand($"SELECT CAST(db_id(N'{_databaseName}') AS int)", connection);
                var dbId = (int?)await sqlCmd.ExecuteScalarAsync();
                return new ConnectionResponse { IsSuccess = true, DatabaseExists = dbId != null, Errors = new List<string>() };
            }
            catch (Exception ex)
            {
                return new ConnectionResponse { IsSuccess = false, Errors = new List<string>() { ex.GetBaseException().Message } };
            }
        }

        public async Task<long> GetPartsCountAsync(IUserContext? userContext)
        {
            var query = $"SELECT SUM(Quantity) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await ExecuteScalarAsync<long>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<long> GetUniquePartsCountAsync(IUserContext? userContext)
        {
            var query = $"SELECT COUNT_BIG(*) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await ExecuteScalarAsync<long>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<decimal> GetPartsValueAsync(IUserContext? userContext)
        {
            var query = $"SELECT SUM(Cost * Quantity) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await ExecuteScalarAsync<decimal>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<PaginatedResponse<Part>> GetLowStockAsync(PaginatedRequest request, IUserContext? userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";

            var parameters = new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            };
            var countQuery = $"SELECT COUNT(*) FROM Parts WHERE Quantity <= LowStockThreshold AND (@UserId IS NULL OR UserId = @UserId);";
            var totalItems = await ExecuteScalarAsync<int>(countQuery, parameters);

            var query =
$@"SELECT * FROM Parts 
WHERE Quantity <= LowStockThreshold AND (@UserId IS NULL OR UserId = @UserId)
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN PartId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartNumber' THEN PartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DigikeyPartNumber' THEN DigikeyPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'MouserPartNumber' THEN MouserPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ArrowPartNumber' THEN ArrowPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Cost' THEN Cost ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Quantity' THEN Quantity ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'LowStockThreshold' THEN LowStockThreshold ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartTypeId' THEN PartTypeId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ProjectId' THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN Location ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber' THEN BinNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber2' THEN BinNumber2 ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Manufacturer' THEN Manufacturer ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ManufacturerPartNumber' THEN ManufacturerPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Part>(query, parameters);
            return new PaginatedResponse<Part>(totalItems, request.Results, request.Page, result);
        }

        public async Task<Part> AddPartAsync(Part part, IUserContext? userContext)
        {
            part.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO Parts (Quantity, LowStockThreshold, PartNumber, PackageType, MountingTypeId, DigiKeyPartNumber, MouserPartNumber, Description, PartTypeId, ProjectId, Keywords, DatasheetUrl, Location, BinNumber, BinNumber2, UserId, Cost, Manufacturer, ManufacturerPartNumber, LowestCostSupplier, LowestCostSupplierUrl, ProductUrl, ImageUrl, DateCreatedUtc, ArrowPartNumber) 
output INSERTED.PartId 
VALUES(@Quantity, @LowStockThreshold, @PartNumber, @PackageType, @MountingTypeId, @DigiKeyPartNumber, @MouserPartNumber, @Description, @PartTypeId, @ProjectId, @Keywords, @DatasheetUrl, @Location, @BinNumber, @BinNumber2, @UserId, @Cost, @Manufacturer, @ManufacturerPartNumber, @LowestCostSupplier, @LowestCostSupplierUrl, @ProductUrl, @ImageUrl, @DateCreatedUtc, @ArrowPartNumber);
";
            return await InsertAsync<Part, long>(query, part, (x, key) => { x.PartId = key; });
        }

        public async Task<Project> AddProjectAsync(Project project, IUserContext? userContext)
        {
            project.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO Projects (Name, Description, Location, Color, UserId, DateCreatedUtc, DateModifiedUtc, Notes) 
output INSERTED.ProjectId 
VALUES(@Name, @Description, @Location, @Color, @UserId, @DateCreatedUtc, @DateModifiedUtc, @Notes);
";
            return await InsertAsync<Project, long>(query, project, (x, key) => { x.ProjectId = key; });
        }

        public async Task<bool> DeletePartAsync(Part part, IUserContext? userContext)
        {
            part.UserId = userContext?.UserId;
            var query = $"DELETE FROM Parts WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, part) > 0;
        }

        public async Task<bool> DeletePartTypeAsync(PartType partType, IUserContext? userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = $"DELETE FROM PartTypes WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, partType) > 0;
        }

        public async Task<bool> DeleteProjectAsync(Project project, IUserContext? userContext)
        {
            project.UserId = userContext?.UserId;
            var query = $"DELETE FROM Projects WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, project) > 0;
        }

        public async Task<ICollection<SearchResult<Part>>> FindPartsAsync(string keywords, IUserContext? userContext)
        {
            // basic ranked search by Michael Brown :)
            var query =
$@"WITH PartsExactMatch (PartId, Rank) AS
(
SELECT PartId, 10 as Rank FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) AND (
PartNumber = @Keywords 
OR DigiKeyPartNumber = @Keywords 
OR MouserPartNumber = @Keywords
OR ArrowPartNumber = @Keywords
OR ManufacturerPartNumber = @Keywords
OR Description = @Keywords 
OR Keywords = @Keywords 
OR Location = @Keywords 
OR BinNumber = @Keywords 
OR BinNumber2 = @Keywords)
),
PartsBeginsWith (PartId, Rank) AS
(
SELECT PartId, 100 as Rank FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) AND (
PartNumber LIKE @Keywords + '%'
OR DigiKeyPartNumber LIKE @Keywords + '%'
OR MouserPartNumber LIKE @Keywords + '%'
OR ArrowPartNumber LIKE @Keywords + '%'
OR ManufacturerPartNumber LIKE @Keywords + '%'
OR Description LIKE @Keywords + '%'
OR Keywords LIKE @Keywords + '%'
OR Location LIKE @Keywords + '%'
OR BinNumber LIKE @Keywords + '%'
OR BinNumber2 LIKE @Keywords+ '%')
),
PartsAny (PartId, Rank) AS
(
SELECT PartId, 200 as Rank FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) AND (
PartNumber LIKE '%' + @Keywords + '%'
OR DigiKeyPartNumber LIKE '%' + @Keywords + '%'
OR MouserPartNumber LIKE '%' + @Keywords + '%'
OR ArrowPartNumber LIKE '%' + @Keywords + '%'
OR ManufacturerPartNumber LIKE '%' + @Keywords + '%'
OR Description LIKE '%' + @Keywords + '%'
OR Keywords LIKE '%' + @Keywords + '%'
OR Location LIKE '%' + @Keywords + '%'
OR BinNumber LIKE '%' + @Keywords + '%'
OR BinNumber2 LIKE '%' + @Keywords + '%')
),
PartsMerged (PartId, Rank) AS
(
	SELECT PartId, Rank FROM PartsExactMatch
	UNION
	SELECT PartId, Rank FROM PartsBeginsWith
	UNION
	SELECT PartId, Rank FROM PartsAny
)
SELECT pm.Rank, p.* FROM Parts p
INNER JOIN (
  SELECT PartId, MIN(Rank) Rank FROM PartsMerged GROUP BY PartId
) pm ON pm.PartId = p.PartId ORDER BY pm.Rank ASC;
;";
            var result = await SqlQueryAsync<PartSearch>(query, new { Keywords = keywords, UserId = userContext?.UserId });
            return result.Select(x => new SearchResult<Part>(x as Part, x.Rank)).OrderBy(x => x.Rank).ToList();
        }

        private async Task<ICollection<OAuthCredential>> GetOAuthCredentialAsync(IUserContext? userContext)
        {
            var query = $"SELECT * FROM OAuthCredentials WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<OAuthCredential?> GetOAuthCredentialAsync(string providerName, IUserContext? userContext)
        {
            var query = $"SELECT * FROM OAuthCredentials WHERE Provider = @ProviderName AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, new { ProviderName = providerName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<PartType?> GetOrCreatePartTypeAsync(PartType partType, IUserContext? userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = $"SELECT PartTypeId FROM PartTypes WHERE Name = @Name AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, partType);
            if (result.Any())
            {
                return result.FirstOrDefault();
            }
            else
            {
                query =
$@"INSERT INTO PartTypes (ParentPartTypeId, Name, UserId, DateCreatedUtc) 
output INSERTED.PartTypeId 
VALUES (@ParentPartTypeId, @Name, @UserId, @DateCreatedUtc);";
                partType = await InsertAsync<PartType, long>(query, partType, (x, key) => { x.PartTypeId = key; });
            }
            return partType;
        }

        public async Task<ICollection<PartType>> GetPartTypesAsync(IUserContext? userContext)
        {
            var query = $"SELECT * FROM PartTypes WHERE (@UserId IS NULL OR UserId = @UserId) OR UserId IS NULL;";
            var result = await SqlQueryAsync<PartType>(query, new { UserId = userContext?.UserId });
            return result.ToList();
        }

        public async Task<Part?> GetPartAsync(long partId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM Parts WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Part>(query, new { PartId = partId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Part?> GetPartAsync(string partNumber, IUserContext? userContext)
        {
            var query = $"SELECT * FROM Parts WHERE PartNumber = @PartNumber AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Part>(query, new { PartNumber = partNumber, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        private async Task<ICollection<Part>> GetPartsAsync()
        {
            var query = $"SELECT * FROM Parts;";
            var result = await SqlQueryAsync<Part>(query);
            return result;
        }

        public async Task<ICollection<Part>> GetPartsAsync(Expression<Func<Part, bool>> predicate, IUserContext? userContext)
        {
            var conditionalQuery = TranslatePredicateToSql(predicate);
            var query = $"SELECT * FROM Parts WHERE {conditionalQuery.Sql} AND (@UserId IS NULL OR UserId = @UserId);";
            conditionalQuery.Parameters.Add("UserId", userContext?.UserId);
            var result = await SqlQueryAsync<Part>(query, conditionalQuery.Parameters);
            return result.ToList();
        }

        private WhereCondition TranslatePredicateToSql(Expression<Func<Part, bool>> predicate)
        {
            var builder = new SqlWhereExpressionBuilder();
            var sql = builder.ToParameterizedSql(predicate);
            return sql;
        }

        public async Task<PaginatedResponse<Part>> GetPartsAsync(PaginatedRequest request, IUserContext? userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var binFilter = "";

            if (request.By != null)
            {
                binFilter = $" AND {request.By} = '{request.Value}'";
            }

            var parameters = new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            };

            var countQuery = $"SELECT COUNT(*) FROM Parts WHERE (@UserId IS NULL OR UserId = @UserId) {binFilter};";
            var totalItems = await ExecuteScalarAsync<int>(countQuery, parameters);

            var query =
$@"SELECT * FROM Parts 
WHERE (@UserId IS NULL OR UserId = @UserId) {binFilter}
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN PartId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartNumber' THEN PartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DigikeyPartNumber' THEN DigikeyPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'MouserPartNumber' THEN MouserPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ArrowPartNumber' THEN ArrowPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Cost' THEN Cost ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Quantity' THEN Quantity ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'LowStockThreshold' THEN LowStockThreshold ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartTypeId' THEN PartTypeId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ProjectId' THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN Location ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber' THEN BinNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber2' THEN BinNumber2 ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Manufacturer' THEN Manufacturer ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ManufacturerPartNumber' THEN ManufacturerPartNumber ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Part>(query, parameters);

            return new PaginatedResponse<Part>(totalItems, request.Results, request.Page, result.ToList());
        }

        public async Task<PartType?> GetPartTypeAsync(long partTypeId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM PartTypes WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, new { PartTypeId = partTypeId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Project?> GetProjectAsync(long projectId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM Projects WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { ProjectId = projectId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Project?> GetProjectAsync(string projectName, IUserContext? userContext)
        {
            var query = $"SELECT * FROM Projects WHERE Name = @Name AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { Name = projectName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        private async Task<ICollection<Project>> GetProjectsAsync(IUserContext? userContext)
        {
            var query = $@"SELECT * FROM Projects WHERE @UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<Project>> GetProjectsAsync(PaginatedRequest request, IUserContext? userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var query =
$@"SELECT * FROM Projects 
WHERE (@UserId IS NULL OR UserId = @UserId) 
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Name' THEN Name ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Description' THEN Description ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN Location ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Project>(query, new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            });
            return result.ToList();
        }

        public async Task RemoveOAuthCredentialAsync(string providerName, IUserContext? userContext)
        {
            var query = $"DELETE FROM OAuthCredentials WHERE Provider = @Provider AND (@UserId IS NULL OR UserId = @UserId);";
            await ExecuteAsync<object>(query, new { Provider = providerName, UserId = userContext?.UserId });
        }

        public async Task<OAuthCredential> SaveOAuthCredentialAsync(OAuthCredential credential, IUserContext? userContext)
        {
            credential.UserId = userContext?.UserId;
            var query = @"SELECT Provider FROM OAuthCredentials WHERE Provider = @Provider AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, credential);
            if (result.Any())
            {
                query = $@"UPDATE OAuthCredentials SET AccessToken = @AccessToken, RefreshToken = @RefreshToken, DateCreatedUtc = @DateCreatedUtc, DateExpiresUtc = @DateExpiresUtc WHERE Provider = @Provider AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync<object>(query, credential);
            }
            else
            {
                query =
$@"INSERT INTO OAuthCredentials (Provider, AccessToken, RefreshToken, DateCreatedUtc, DateExpiresUtc, UserId) 
VALUES (@Provider, @AccessToken, @RefreshToken, @DateCreatedUtc, @DateExpiresUtc, @UserId);";
                await InsertAsync<OAuthCredential, string>(query, credential, (x, key) => { });
            }
            return credential;
        }

        public async Task<Part> UpdatePartAsync(Part part, IUserContext? userContext)
        {
            part.UserId = userContext?.UserId;
            var query = $"SELECT PartId FROM Parts WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Part>(query, part);
            if (result.Any())
            {
                query = $"UPDATE Parts SET Quantity = @Quantity, LowStockThreshold = @LowStockThreshold, Cost = @Cost, PartNumber = @PartNumber, PackageType = @PackageType, MountingTypeId = @MountingTypeId, DigiKeyPartNumber = @DigiKeyPartNumber, MouserPartNumber = @MouserPartNumber, Description = @Description, PartTypeId = @PartTypeId, ProjectId = @ProjectId, Keywords = @Keywords, DatasheetUrl = @DatasheetUrl, Location = @Location, BinNumber = @BinNumber, BinNumber2 = @BinNumber2, ProductUrl = @ProductUrl, ImageUrl = @ImageUrl, LowestCostSupplier = @LowestCostSupplier, LowestCostSupplierUrl = @LowestCostSupplierUrl, Manufacturer = @Manufacturer, ManufacturerPartNumber = @ManufacturerPartNumber, ArrowPartNumber = @ArrowPartNumber WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, part);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(Part)} = {part.PartId}");
            }
            return part;
        }

        public async Task<PartType> UpdatePartTypeAsync(PartType partType, IUserContext? userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = $"SELECT PartTypeId FROM PartTypes WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, partType);
            if (result.Any())
            {
                query = $"UPDATE PartTypes SET Name = @Name, ParentPartTypeId = @ParentPartTypeId WHERE PartTypeId = @PartTypeId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, partType);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(PartType)} = {partType.PartTypeId}");
            }
            return partType;
        }

        public async Task<Project> UpdateProjectAsync(Project project, IUserContext? userContext)
        {
            project.UserId = userContext?.UserId;
            var query = $"SELECT ProjectId FROM Projects WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Project>(query, project);
            if (result.Any())
            {
                query = $"UPDATE Projects SET Name = @Name, Description = @Description, Location = @Location, Color = @Color, DateModifiedUtc = @DateModifiedUtc, Notes = @Notes WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, project);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(Project)} = {project.ProjectId}");
            }
            return project;
        }

        public async Task<StoredFile> AddStoredFileAsync(StoredFile storedFile, IUserContext? userContext)
        {
            storedFile.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO StoredFiles (FileName, OriginalFileName, StoredFileType, PartId, FileLength, Crc32, UserId, DateCreatedUtc) 
output INSERTED.StoredFileId 
VALUES(@FileName, @OriginalFileName, @StoredFileType, @PartId, @FileLength, @Crc32, @UserId, @DateCreatedUtc);
";
            return await InsertAsync<StoredFile, long>(query, storedFile, (x, key) => { x.StoredFileId = key; });
        }

        public async Task<StoredFile?> GetStoredFileAsync(long storedFileId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM StoredFiles WHERE StoredFileId = @StoredFileId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<StoredFile>(query, new { StoredFileId = storedFileId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<StoredFile?> GetStoredFileAsync(string filename, IUserContext? userContext)
        {
            var query = $"SELECT * FROM StoredFiles WHERE Filename = @Filename AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<StoredFile>(query, new { Filename = filename, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ICollection<StoredFile>> GetStoredFilesAsync(IUserContext? userContext)
        {
            var query = $@"SELECT * FROM StoredFiles WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<StoredFile>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<StoredFile>> GetStoredFilesAsync(long partId, StoredFileType? fileType, IUserContext? userContext)
        {
            var query = $@"SELECT * FROM StoredFiles WHERE PartId = @PartId AND (@StoredFileType IS NULL OR StoredFileType = @StoredFileType) AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<StoredFile>(query, new { PartId = partId, StoredFileType = fileType, UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<StoredFile>> GetStoredFilesAsync(PaginatedRequest request, IUserContext? userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var query =
$@"SELECT * FROM StoredFiles 
WHERE (@UserId IS NULL OR UserId = @UserId) 
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN StoredFileId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'FileName' THEN FileName ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'OriginalFileName' THEN OriginalFileName ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'StoredFileType' THEN StoredFileType ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartId' THEN PartId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'FileLength' THEN FileLength ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Crc32' THEN Crc32 ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<StoredFile>(query, new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            });
            return result.ToList();
        }

        public async Task<bool> DeleteStoredFileAsync(StoredFile storedFile, IUserContext? userContext)
        {
            storedFile.UserId = userContext?.UserId;
            var query = $"DELETE FROM StoredFiles WHERE StoredFileId = @StoredFileId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, storedFile) > 0;
        }

        public async Task<StoredFile> UpdateStoredFileAsync(StoredFile storedFile, IUserContext? userContext)
        {
            storedFile.UserId = userContext?.UserId;
            var query = $"SELECT StoredFileId FROM StoredFiles WHERE StoredFileId = @StoredFileId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<StoredFile>(query, storedFile);
            if (result.Any())
            {
                query = $"UPDATE StoredFiles SET FileName = @FileName, OriginalFileName = @OriginalFileName, StoredFileType = @StoredFileType, PartId = @PartId, FileLength = @FileLength, Crc32 = @Crc32 WHERE StoredFileId = @StoredFileId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, storedFile);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(StoredFile)} = {storedFile.StoredFileId}");
            }
            return storedFile;
        }

        public async Task<OAuthAuthorization> CreateOAuthRequestAsync(OAuthAuthorization authRequest, IUserContext? userContext)
        {
            var oAuthRequest = new OAuthRequest
            {
                AuthorizationCode = authRequest.AuthorizationCode,
                AuthorizationReceived = authRequest.AuthorizationReceived,
                Error = authRequest.Error,
                ErrorDescription = authRequest.ErrorDescription,
                Provider = authRequest.Provider,
                RequestId = authRequest.Id,
                ReturnToUrl = authRequest.ReturnToUrl,
                UserId = userContext?.UserId,
                DateCreatedUtc = DateTime.UtcNow,
                DateModifiedUtc = DateTime.UtcNow
            };
            var query =
$@"INSERT INTO OAuthRequests (AuthorizationCode, AuthorizationReceived, Error, ErrorDescription, Provider, RequestId, ReturnToUrl, UserId, DateCreatedUtc, DateModifiedUtc) 
output INSERTED.OAuthRequestId 
VALUES(@AuthorizationCode, @AuthorizationReceived, @Error, @ErrorDescription, @Provider, @RequestId, @ReturnToUrl, @UserId, @DateCreatedUtc, @DateModifiedUtc);
";
            var createdOAuthRequest = await InsertAsync<OAuthRequest, int>(query, oAuthRequest, (x, key) => { x.OAuthRequestId = key; });
            return authRequest;
        }

        public async Task<OAuthAuthorization> UpdateOAuthRequestAsync(OAuthAuthorization authRequest, IUserContext? userContext)
        {
            var oAuthRequest = new OAuthRequest
            {
                AuthorizationCode = authRequest.AuthorizationCode,
                AuthorizationReceived = authRequest.AuthorizationReceived,
                Error = authRequest.Error,
                ErrorDescription = authRequest.ErrorDescription,
                Provider = authRequest.Provider,
                RequestId = authRequest.Id,
                ReturnToUrl = authRequest.ReturnToUrl,
                UserId = userContext?.UserId,
                DateModifiedUtc = DateTime.UtcNow
            };
            var query = $"SELECT OAuthRequestId FROM OAuthRequests WHERE Provider = @Provider AND RequestId = @RequestId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthRequest>(query, oAuthRequest);
            if (result.Any())
            {
                query = $"UPDATE OAuthRequests SET AuthorizationCode = @AuthorizationCode, AuthorizationReceived = @AuthorizationReceived, Error = @Error, ErrorDescription = @ErrorDescription, DateModifiedUtc = @DateModifiedUtc WHERE Provider = @Provider AND RequestId = @RequestId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, oAuthRequest);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(OAuthRequest)} = (Provider: {oAuthRequest.Provider}, RequestId: {oAuthRequest.RequestId})");
            }
            return authRequest;
        }

        public async Task<ICollection<OAuthRequest>> GetOAuthRequestsAsync(IUserContext? userContext)
        {
            var query = $"SELECT * FROM OAuthRequests WHERE (@UserId IS NULL OR UserId = @UserId);";
            return await SqlQueryAsync<OAuthRequest>(query, new { UserId = userContext?.UserId });
        }

        public async Task<OAuthAuthorization?> GetOAuthRequestAsync(Guid requestId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM OAuthRequests WHERE RequestId = @RequestId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<OAuthRequest>(query, new { RequestId = requestId, UserId = userContext?.UserId });
            var oAuthRequest = result.FirstOrDefault();
            if (oAuthRequest == null) return null;

            return new OAuthAuthorization(oAuthRequest.Provider, oAuthRequest.RequestId)
            {
                UserId = userContext?.UserId,
                Error = oAuthRequest.Error ?? string.Empty,
                ErrorDescription = oAuthRequest.ErrorDescription ?? string.Empty,
                AuthorizationReceived = false,
                ReturnToUrl = oAuthRequest.ReturnToUrl ?? string.Empty,
            };
        }

        #region BinnerDb V4

        public async Task<Pcb?> GetPcbAsync(long pcbId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM Pcbs WHERE PcbId = @PcbId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Pcb>(query, new { PcbId = pcbId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ICollection<Pcb>> GetPcbsAsync(long projectId, IUserContext? userContext)
        {
            var query = $"SELECT p.* FROM ProjectPcbAssignments a INNER JOIN Pcbs p ON p.PcbId=a.PcbId WHERE a.ProjectId=@ProjectId AND (@UserId IS NULL OR p.UserId = @UserId);";
            var result = await SqlQueryAsync<Pcb>(query, new { ProjectId = projectId, UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<Pcb>> GetPcbsAsync(IUserContext? userContext)
        {
            var query = $"SELECT * FROM Pcbs WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Pcb>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<Pcb> AddPcbAsync(Pcb pcb, IUserContext? userContext)
        {
            pcb.UserId = userContext?.UserId;
            var query =
                $@"INSERT INTO Pcbs (Name, Description, SerialNumberFormat, LastSerialNumber, UserId, DateCreatedUtc, DateModifiedUtc) 
output INSERTED.PcbId 
VALUES(@Name, @Description, @SerialNumberFormat, @LastSerialNumber, @UserId, @DateCreatedUtc, @DateModifiedUtc);
";
            return await InsertAsync<Pcb, long>(query, pcb, (x, key) => { x.PcbId = key; });
        }

        public async Task<Pcb> UpdatePcbAsync(Pcb pcb, IUserContext? userContext)
        {
            if (pcb == null) throw new ArgumentNullException(nameof(pcb));
            pcb.UserId = userContext?.UserId;
            var query = $"SELECT PcbId FROM Pcbs WHERE PcbId = @PcbId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Pcb>(query, pcb);
            if (result.Any())
            {
                query = $"UPDATE Pcbs SET Name = @Name, Description = @Description, SerialNumberFormat = @SerialNumberFormat, LastSerialNumber = @LastSerialNumber, DateModifiedUtc = @DateModifiedUtc WHERE PcbId = @PcbId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, pcb);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(Pcb)} = {pcb.PcbId}");
            }
            return pcb;
        }

        public async Task<bool> DeletePcbAsync(Pcb pcb, IUserContext? userContext)
        {
            pcb.UserId = userContext?.UserId;
            var query = $"DELETE FROM Pcbs WHERE PcbId = @PcbId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, pcb) > 0;
        }

        public async Task<PcbStoredFileAssignment?> GetPcbStoredFileAssignmentAsync(long pcbStoredFileAssignmentId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM PcbStoredFileAssignments WHERE PcbStoredFileAssignmentId = @PcbStoredFileAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PcbStoredFileAssignment>(query, new { PcbStoredFileAssignmentId = pcbStoredFileAssignmentId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ICollection<PcbStoredFileAssignment>> GetPcbStoredFileAssignmentsAsync(IUserContext? userContext)
        {
            var query = $@"SELECT * FROM PcbStoredFileAssignments WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PcbStoredFileAssignment>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<PcbStoredFileAssignment>> GetPcbStoredFileAssignmentsAsync(long pcbId, IUserContext? userContext)
        {
            var query = $@"SELECT * FROM PcbStoredFileAssignments WHERE PcbId = @PcbId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PcbStoredFileAssignment>(query, new { PcbId = pcbId, UserId = userContext?.UserId });
            return result;
        }

        public async Task<PcbStoredFileAssignment> AddPcbStoredFileAssignmentAsync(PcbStoredFileAssignment assignment, IUserContext? userContext)
        {
            assignment.UserId = userContext?.UserId;
            var query =
                $@"INSERT INTO PcbStoredFileAssignments (PcbId, StoredFileId, Name, Notes, UserId, DateCreatedUtc, DateModifiedUtc) 
output INSERTED.PcbStoredFileAssignmentId 
VALUES(@PcbId, @StoredFileId, @Name, @Notes, @UserId, @DateCreatedUtc, @DateModifiedUtc);
";
            return await InsertAsync<PcbStoredFileAssignment, long>(query, assignment, (x, key) => { x.PcbStoredFileAssignmentId = key; });
        }

        public async Task<PcbStoredFileAssignment> UpdatePcbStoredFileAssignmentAsync(PcbStoredFileAssignment assignment, IUserContext? userContext)
        {
            if (assignment == null) throw new ArgumentNullException(nameof(assignment));
            assignment.UserId = userContext?.UserId;
            var query = $"SELECT PcbStoredFileAssignmentId FROM PcbStoredFileAssignments WHERE PcbStoredFileAssignmentId = @PcbStoredFileAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<Pcb>(query, assignment);
            if (result.Any())
            {
                query = $"UPDATE PcbStoredFileAssignments SET PcbId = @PcbId, StoredFileId = @StoredFileId, Name = @Name, Notes = @Notes, DateModifiedUtc = @DateModifiedUtc WHERE PcbStoredFileAssignmentId = @PcbStoredFileAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, assignment);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(PcbStoredFileAssignment)} = {assignment.PcbStoredFileAssignmentId}");
            }
            return assignment;
        }

        public async Task<bool> RemovePcbStoredFileAssignmentAsync(PcbStoredFileAssignment assignment, IUserContext? userContext)
        {
            assignment.UserId = userContext?.UserId;
            var query = $"DELETE FROM PcbStoredFileAssignments WHERE PcbStoredFileAssignmentId = @PcbStoredFileAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, assignment) > 0;
        }

        public async Task<ICollection<ProjectPartAssignment>> GetPartAssignmentsAsync(long partId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM ProjectPartAssignments WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new { PartId = partId, UserId = userContext?.UserId });
            return result;
        }

        public async Task<ProjectPartAssignment?> GetProjectPartAssignmentAsync(long projectPartAssignmentId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM ProjectPartAssignments WHERE ProjectPartAssignmentId = @ProjectPartAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new { ProjectPartAssignmentId = projectPartAssignmentId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ProjectPartAssignment?> GetProjectPartAssignmentAsync(long projectId, long partId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM ProjectPartAssignments WHERE ProjectId = @ProjectId AND PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new { ProjectId = projectId, PartId = partId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ProjectPartAssignment?> GetProjectPartAssignmentAsync(long projectId, string partName, IUserContext? userContext)
        {
            var query = $"SELECT * FROM ProjectPartAssignments WHERE ProjectId = @ProjectId AND PartName = @PartName AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new { ProjectId = projectId, PartName = partName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ICollection<ProjectPartAssignment>> GetProjectPartAssignmentsAsync(IUserContext? userContext)
        {
            var query = $@"SELECT * FROM ProjectPartAssignments WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<ProjectPartAssignment>> GetProjectPartAssignmentsAsync(long projectId, IUserContext? userContext)
        {
            var query = $@"SELECT * FROM ProjectPartAssignments WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new { ProjectId = projectId, UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<ProjectPartAssignment>> GetProjectPartAssignmentsAsync(long projectId, PaginatedRequest request, IUserContext? userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var query =
                $@"SELECT * FROM ProjectPartAssignments 
WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId) 
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN ProjectId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartId' THEN PartId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PcbId' THEN PcbId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartName' THEN PartName ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Notes' THEN Notes ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ReferenceId' THEN ReferenceId ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN DateCreatedUtc ELSE NULL END {sortDirection},
CASE WHEN @OrderBy = 'DateModifiedUtc' THEN DateModifiedUtc ELSE NULL END {sortDirection},
CASE WHEN @OrderBy = 'QuantityAvailable' THEN QuantityAvailable ELSE NULL END {sortDirection}
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, new
            {
                ProjectId = projectId,
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = request.Direction,
                UserId = userContext?.UserId
            });
            return result.ToList();
        }

        public async Task<ProjectPartAssignment> AddProjectPartAssignmentAsync(ProjectPartAssignment assignment, IUserContext? userContext)
        {
            assignment.UserId = userContext?.UserId;
            var query =
                $@"INSERT INTO ProjectPartAssignments (ProjectId, PartId, PcbId, PartName, Quantity, Notes, ReferenceId, UserId, DateCreatedUtc, DateModifiedUtc, QuantityAvailable) 
output INSERTED.ProjectPartAssignmentId 
VALUES(@ProjectId, @PartId, @PcbId, @PartName, @Quantity, @Notes, @ReferenceId, @UserId, @DateCreatedUtc, @DateModifiedUtc, @QuantityAvailable);
";
            return await InsertAsync<ProjectPartAssignment, long>(query, assignment, (x, key) => { x.ProjectPartAssignmentId = key; });
        }

        public async Task<ProjectPartAssignment> UpdateProjectPartAssignmentAsync(ProjectPartAssignment assignment, IUserContext? userContext)
        {
            if (assignment == null) throw new ArgumentNullException(nameof(assignment));
            assignment.UserId = userContext?.UserId;
            var query = $"SELECT ProjectPartAssignmentId FROM ProjectPartAssignments WHERE ProjectPartAssignmentId = @ProjectPartAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPartAssignment>(query, assignment);
            if (result.Any())
            {
                query = $"UPDATE ProjectPartAssignments SET ProjectId = @ProjectId, PartId = @PartId, PcbId = @PcbId, PartName = @PartName, Quantity = @Quantity, Notes = @Notes, ReferenceId = @ReferenceId, DateModifiedUtc = @DateModifiedUtc, QuantityAvailable = @QuantityAvailable WHERE ProjectPartAssignmentId = @ProjectPartAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, assignment);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(ProjectPartAssignment)} = {assignment.ProjectPartAssignmentId}");
            }
            return assignment;
        }

        public async Task<bool> RemoveProjectPartAssignmentAsync(ProjectPartAssignment assignment, IUserContext? userContext)
        {
            assignment.UserId = userContext?.UserId;
            var query = $"DELETE FROM ProjectPartAssignments WHERE ProjectPartAssignmentId = @ProjectPartAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, assignment) > 0;
        }

        public async Task<ProjectPcbAssignment?> GetProjectPcbAssignmentAsync(long projectPcbAssignmentId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM ProjectPcbAssignments WHERE ProjectPcbAssignmentId = @ProjectPcbAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPcbAssignment>(query, new { ProjectPcbAssignmentId = projectPcbAssignmentId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ICollection<ProjectPcbAssignment>> GetProjectPcbAssignmentsAsync(IUserContext? userContext)
        {
            var query = $@"SELECT * FROM ProjectPcbAssignments WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPcbAssignment>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<ProjectPcbAssignment>> GetProjectPcbAssignmentsAsync(long projectId, IUserContext? userContext)
        {
            var query = $@"SELECT * FROM ProjectPcbAssignments WHERE ProjectId = @ProjectId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPcbAssignment>(query, new { ProjectId = projectId, UserId = userContext?.UserId });
            return result;
        }

        public async Task<ProjectPcbAssignment> AddProjectPcbAssignmentAsync(ProjectPcbAssignment assignment, IUserContext? userContext)
        {
            assignment.UserId = userContext?.UserId;
            var query =
                $@"INSERT INTO ProjectPcbAssignments (ProjectId, PcbId, UserId, DateCreatedUtc) 
output INSERTED.ProjectPcbAssignmentId 
VALUES(@ProjectId, @PcbId, @UserId, @DateCreatedUtc);
";
            return await InsertAsync<ProjectPcbAssignment, long>(query, assignment, (x, key) => { x.ProjectPcbAssignmentId = key; });
        }

        public async Task<ProjectPcbAssignment> UpdateProjectPcbAssignmentAsync(ProjectPcbAssignment assignment, IUserContext? userContext)
        {
            if (assignment == null) throw new ArgumentNullException(nameof(assignment));
            assignment.UserId = userContext?.UserId;
            var query = $"SELECT ProjectPcbAssignmentId FROM ProjectPcbAssignments WHERE ProjectPcbAssignmentId = @ProjectPcbAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<ProjectPcbAssignment>(query, assignment);
            if (result.Any())
            {
                query = $"UPDATE ProjectPcbAssignments SET ProjectId = @ProjectId, PcbId = @PcbId, DateModifiedUtc = @DateModifiedUtc WHERE ProjectPcbAssignmentId = @ProjectPcbAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, assignment);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(ProjectPcbAssignment)} = {assignment.ProjectPcbAssignmentId}");
            }
            return assignment;
        }

        public async Task<bool> RemoveProjectPcbAssignmentAsync(ProjectPcbAssignment assignment, IUserContext? userContext)
        {
            assignment.UserId = userContext?.UserId;
            var query = $"DELETE FROM ProjectPcbAssignments WHERE ProjectPcbAssignmentId = @ProjectPcbAssignmentId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, assignment) > 0;
        }

        #endregion

        #region BinnerDb V5

        public async Task<PartSupplier> AddPartSupplierAsync (PartSupplier partSupplier, IUserContext? userContext)
        {
            partSupplier.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO PartSuppliers (PartId, Name, SupplierPartNumber, Cost, QuantityAvailable, MinimumOrderQuantity, ProductUrl, ImageUrl, DateCreatedUtc, DateModifiedUtc, UserId) 
output INSERTED.PartSupplierId 
VALUES(@PartId, @Name, @SupplierPartNumber, @Cost, @QuantityAvailable, @MinimumOrderQuantity, @ProductUrl, @ImageUrl, @DateCreatedUtc, @DateModifiedUtc, @UserId);
";
            return await InsertAsync<PartSupplier, long>(query, partSupplier, (x, key) => { x.PartSupplierId = key; });
        }

        public async Task<PartSupplier?> GetPartSupplierAsync(long partSupplierId, IUserContext? userContext)
        {
            var query = $"SELECT * FROM PartSuppliers WHERE PartSupplierId = @PartSupplierId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartSupplier>(query, new { PartSupplierId = partSupplierId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<ICollection<PartSupplier>> GetPartSuppliersAsync(IUserContext? userContext)
        {
            var query = $@"SELECT * FROM PartSuppliers WHERE (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartSupplier>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<PartSupplier>> GetPartSuppliersAsync(long partId, IUserContext? userContext)
        {
            var query = $@"SELECT * FROM PartSuppliers WHERE PartId = @PartId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartSupplier>(query, new { PartId = partId, UserId = userContext?.UserId });
            return result;
        }

        public async Task<bool> DeletePartSupplierAsync(PartSupplier partSupplier, IUserContext? userContext)
        {
            partSupplier.UserId = userContext?.UserId;
            var query = $"DELETE FROM PartSuppliers WHERE PartSupplierId = @PartSupplierId AND (@UserId IS NULL OR UserId = @UserId);";
            return await ExecuteAsync(query, partSupplier) > 0;
        }

        public async Task<PartSupplier> UpdatePartSupplierAsync(PartSupplier partSupplier, IUserContext? userContext)
        {
            partSupplier.UserId = userContext?.UserId;
            var query = $"SELECT PartSupplierId FROM PartSuppliers WHERE PartSupplierId = @PartSupplierId AND (@UserId IS NULL OR UserId = @UserId);";
            var result = await SqlQueryAsync<PartSupplier>(query, partSupplier);
            if (result.Any())
            {
                query = $"UPDATE PartSuppliers SET PartId = @PartId, Name = @Name, SupplierPartNumber = @SupplierPartNumber, Cost = @Cost, QuantityAvailable = @QuantityAvailable, MinimumOrderQuantity = @MinimumOrderQuantity, ProductUrl = @ProductUrl, ImageUrl = @ImageUrl, DateModifiedUtc = @DateModifiedUtc WHERE PartSupplierId = @PartSupplierId AND (@UserId IS NULL OR UserId = @UserId);";
                await ExecuteAsync(query, partSupplier);
            }
            else
            {
                throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Record not found for {nameof(PartSupplier)} = {partSupplier.PartSupplierId}");
            }
            return partSupplier;
        }

        #endregion

        private async Task<T> InsertAsync<T, TKey>(string query, T parameters, Action<T, TKey> keySetter)
        {
            using (var connection = new SqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters<T>(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var result = await sqlCmd.ExecuteScalarAsync();
                    if (result != null)
                        keySetter.Invoke(parameters, (TKey)result);
                }
                connection.Close();
            }
            return parameters;
        }

        private async Task<ICollection<T>> SqlQueryAsync<T>(string query, object? parameters = null)
        {
            var results = new List<T>();
            var type = typeof(T).GetExtendedType();
            using (var connection = new SqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    if (parameters != null)
                        sqlCmd.Parameters.AddRange(CreateParameters(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var reader = await sqlCmd.ExecuteReaderAsync();
                    while (reader.Read())
                    {
                        var newObj = Activator.CreateInstance<T>();
                        foreach (var prop in type.Properties)
                        {
                            if (reader.HasColumn(prop.Name))
                            {
                                var val = MapToPropertyValue(reader[prop.Name], prop.Type);
                                newObj.SetPropertyValue(prop.PropertyInfo, val);
                            }
                        }
                        results.Add(newObj);
                    }
                }
                connection.Close();
            }
            return results;
        }

        private async Task<T?> ExecuteScalarAsync<T>(string query, object? parameters = null)
        {
            T? result;
            using (var connection = new SqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var untypedResult = await sqlCmd.ExecuteScalarAsync();
                    if (untypedResult != DBNull.Value)
                        result = (T?)untypedResult;
                    else
                        return default(T);
                }
                connection.Close();
            }
            return result;
        }

        private async Task<int> ExecuteAsync<T>(string query, T record)
        {
            var modified = 0;
            using (var connection = new SqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters<T>(record));
                    sqlCmd.CommandType = CommandType.Text;
                    modified = await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return modified;
        }

        private SqlParameter[] CreateParameters<T>(T record)
        {
            var parameters = new List<SqlParameter>();
            var extendedType = record.GetExtendedType();
            if (extendedType.IsDictionary)
            {
                var t = record as IDictionary<string, object>;
                if (t != null)
                {
                    foreach (var p in t)
                    {
                        var key = p.Key;
                        var val = p.Value;
                        var propertyMapped = MapFromPropertyValue(val);
                        parameters.Add(new SqlParameter(key.ToString(), propertyMapped));
                    }
                }
            }
            else
            {
                var properties = record.GetProperties(PropertyOptions.HasGetter);
                foreach (var property in properties)
                {
                    var propertyValue = record.GetPropertyValue(property);
                    var propertyMapped = MapFromPropertyValue(propertyValue);
                    parameters.Add(new SqlParameter(property.Name, propertyMapped));
                }
            }
            return parameters.ToArray();
        }

        private static object? MapToPropertyValue(object? obj, Type destinationType)
        {
            if (obj == DBNull.Value || obj == null) return null;

            var objType = destinationType.GetExtendedType();
            switch (objType)
            {
                case var p when p.IsCollection:
                case var a when a.IsArray:
                    return obj.ToString()?.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                default:
                    return obj;
            }
        }

        private static object? MapFromPropertyValue(object? obj)
        {
            if (obj == null) return DBNull.Value;

            var objType = obj.GetExtendedType();
            switch (objType)
            {
                case var p when p.IsCollection:
                case var a when a.IsArray:
                    return string.Join(",", ((ICollection<string>)obj).Select(x => x.ToString()).ToArray());
                case var p when p.Type == typeof(DateTime):
                    if (((DateTime)obj) == DateTime.MinValue)
                        return SqlDateTime.MinValue.Value;
                    return obj;
                default:
                    return obj;
            }
        }

        private async Task<bool> GenerateDatabaseIfNotExistsAsync<T>()
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(_config.ConnectionString);
            _databaseName = !string.IsNullOrEmpty(connectionStringBuilder.InitialCatalog) ? connectionStringBuilder.InitialCatalog : "Binner";
            var schemaGenerator = new SqlServerSchemaGenerator<T>(_databaseName);
            var modified = 0;
            // Ensure database exists
            var query = schemaGenerator.CreateDatabaseIfNotExists();
            using (var connection = new SqlConnection(GetMasterDbConnectionString(_config.ConnectionString)))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    modified = (int)(await sqlCmd.ExecuteScalarAsync() ?? 0);
                }
                connection.Close();
            }
            // Ensure table schema exists
            query = schemaGenerator.CreateTableSchemaIfNotExists();
            using (var connection = new SqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    modified = (int)(await sqlCmd.ExecuteScalarAsync() ?? 0);
                }
                connection.Close();
            }
            if (modified > 0) await SeedInitialDataAsync();

            return modified > 0;
        }

        private async Task<bool> SeedInitialDataAsync()
        {
            //DefaultPartTypes
            var defaultPartTypes = typeof(SystemDefaults.DefaultPartTypes).GetExtendedType();
            var query = "";
            var modified = 0;
            foreach (var partType in defaultPartTypes.EnumValues)
            {
                int? parentPartTypeId = null;
                var partTypeEnum = (DefaultPartTypes)partType.Key;
                var field = typeof(DefaultPartTypes).GetField(partType.Value);
                if (field != null && field.IsDefined(typeof(ParentPartTypeAttribute), false))
                {
                    var customAttribute = Attribute.GetCustomAttribute(field, typeof(ParentPartTypeAttribute)) as ParentPartTypeAttribute;
                    if (customAttribute != null)
                        parentPartTypeId = (int)customAttribute.Parent;
                }

                query += $"INSERT INTO PartTypes (Name, ParentPartTypeId, DateCreatedUtc) VALUES('{partType.Value}', {parentPartTypeId?.ToString() ?? "null"}, GETUTCDATE());\r\n";
            }
            using (var connection = new SqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new SqlCommand(query, connection))
                {
                    modified = await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return modified > 0;
        }

        private static string GetMasterDbConnectionString(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "master"
            };
            return builder.ToString();
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool isDisposing)
        {
            if (_isDisposed)
                return;
            if (isDisposing)
            {

            }
            _isDisposed = true;
        }
    }
}
