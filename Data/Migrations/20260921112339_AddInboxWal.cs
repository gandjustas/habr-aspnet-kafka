using Microsoft.EntityFrameworkCore.Migrations;
using NpgsqlTypes;

#nullable disable

namespace Web.Migrations
{
    /// <inheritdoc />
    public partial class AddInboxWal : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "inbox_wal",
                columns: table => new
                {
                    wal = table.Column<NpgsqlLogSequenceNumber>(type: "pg_lsn", nullable: false),
                    slot = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_inbox_wal", x => x.wal);
                })
                .Annotation("Npgsql:StorageParameter:autovacuum_enabled", false);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "inbox_wal");
        }
    }
}
