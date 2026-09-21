using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.FileProviders;

// То, чего нет в модели EF (публикации, процедуры, партиционирование), лежит по файлу на объект
// в Migrations/<ИмяМиграции>/ и подхватывается отсюда по nameof, чтобы переименование не ломало связь
internal static class MigrationSql
{
    private static readonly ManifestEmbeddedFileProvider Files =
        new(typeof(MigrationSql).Assembly, "Migrations");

    public static void Run(MigrationBuilder migrationBuilder, string migrationName)
    {
        foreach (var file in Files.GetDirectoryContents(migrationName))
        {
            using var reader = new StreamReader(file.CreateReadStream());
            migrationBuilder.Sql(reader.ReadToEnd());
        }
    }
}
