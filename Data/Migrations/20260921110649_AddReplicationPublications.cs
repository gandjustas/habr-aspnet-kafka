using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Web.Migrations
{
    /// <inheritdoc />
    public partial class AddReplicationPublications : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            MigrationSql.Run(migrationBuilder, nameof(AddReplicationPublications));
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("DROP PUBLICATION IF EXISTS load_test_pub;");
            migrationBuilder.Sql("DROP PUBLICATION IF EXISTS rep_pub;");
        }
    }
}
