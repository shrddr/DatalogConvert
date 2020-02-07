using libDAT;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dat2sql
{
    public static class Globals
    {
        public const int BATCH_SIZE = 1000;
    }

    class Program
    {
        static int Offset(int record) { return 0x121 + 39 * record; }

        public static bool TableExists(SqlConnection conn, string TableName)
        {
            string sql = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}'";
            SqlCommand cmd = new SqlCommand(sql, conn);
            int count = Convert.ToInt32(cmd.ExecuteScalar());
            return count > 0;
        }

        public static bool DropTable(SqlConnection conn, string TableName)
        {
            Console.WriteLine("Dropping table: " + TableName);
            string sql = $"DROP TABLE [{TableName}]";
            SqlCommand cmd = new SqlCommand(sql, conn);
            int count = Convert.ToInt32(cmd.ExecuteScalar());
            return count > 0;
        }

        public static bool CreateTables(SqlConnection conn, string TagTable, string FloatTable)
        {
            bool res = false;
            try
            {
                if (TableExists(conn, FloatTable))
                {
                    DropTable(conn, FloatTable);
                }
                Console.WriteLine("Creating table: " + FloatTable);
                string query = $"CREATE TABLE [{FloatTable}] ([DateAndTime] [datetime] NULL, [Millitm] [smallint] NULL, [TagIndex] [smallint] NULL, [Val] [float] NULL, [Status] [nvarchar] (1) NULL, [Marker] [nvarchar] (1) NULL)"; 
                SqlCommand cmd = new SqlCommand(query, conn);
                int affected = cmd.ExecuteNonQuery();
                res &= affected > 0;

                if (TableExists(conn, TagTable))
                {
                    DropTable(conn, TagTable);
                }
                Console.WriteLine("Creating table: " + TagTable);
                query = $"CREATE TABLE [{TagTable}] ([TagName] [nvarchar](255) NULL, [TagIndex] [smallint] NULL, [TagType] [smallint] NULL, [TagDataType] [smallint] NULL)";
                cmd = new SqlCommand(query, conn);
                affected = cmd.ExecuteNonQuery();
                res &= affected > 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating tables: " + ex.Message);
            }
            return res;
        }

        static void Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine("Error: argument not found");
                Console.WriteLine("Usage: dat2sql ServerName CatalogName User Password [TablePrefix]");
                return;
            }

            string FloatTable = "FloatTable";
            string TagTable = "TagTable";
            string StringTable = "StringTable";
            if (args.Length >= 5)
            {
                FloatTable = $"{args[4]}_FloatTable";
                TagTable = $"{args[4]}_TagTable";
                StringTable = $"{args[4]}_StringTable";
            }

            string connString = $"Data Source={args[0]};Initial Catalog={args[1]};Persist Security Info=True;User ID={args[2]};Password={args[3]}";

            SqlConnection conn = new SqlConnection(connString);
            conn.Open();
            CreateTables(conn, TagTable, FloatTable);

            DatReader dr = new DatReader(".");
            uint converted = 0;
            foreach (string infilename in dr.GetFloatfiles())
            {
                Console.WriteLine("converting {0}", infilename);
                MakeSQL(dr, infilename, conn, TagTable, FloatTable);
                converted++;
            }

            conn.Close();
            Console.WriteLine("converted {0} files", converted);
        }

        public static void MakeSQL(DatReader dr, string floatfile_name, SqlConnection conn, string TagTable, string FloatTable)
        {
            Dictionary<int, int> sql_tagids = new Dictionary<int, int>();

            string query_select = $"SELECT [TagIndex],[TagType],[TagDataType] FROM [{TagTable}] WHERE TagName = @1";
            SqlCommand cmd_select = new SqlCommand(query_select, conn);
            cmd_select.Parameters.Add("@1", SqlDbType.NVarChar, 255);

            string query_insert = $"INSERT INTO [{TagTable}] VALUES (@1,@2,@3,@4)";
            SqlCommand cmd_insert = new SqlCommand(query_insert, conn);
            cmd_insert.Parameters.Add("@1", SqlDbType.NVarChar, 255);
            cmd_insert.Parameters.Add("@2", SqlDbType.SmallInt);
            cmd_insert.Parameters.Add("@3", SqlDbType.SmallInt);
            cmd_insert.Parameters.Add("@4", SqlDbType.SmallInt);

            string tagname_filename = floatfile_name.Replace(" (Float)", " (Tagname)");

            foreach (DatTagRecord tag in dr.ReadTagFile(floatfile_name))
            {
                bool tag_exists = false;
                cmd_select.Parameters["@1"].Value = tag.name;
                SqlDataReader reader = cmd_select.ExecuteReader();
                if (reader.HasRows)
                {
                    reader.Read();
                    Int16 ti = reader.GetInt16(0);
                    byte tt = (byte)reader.GetInt16(1);
                    Int16 td = reader.GetInt16(2);
                    if (tt == tag.type && td == tag.dtype)
                    {
                        tag_exists = true;
                        sql_tagids[tag.id] = ti;
                    }
                }
                reader.Close();
                if (!tag_exists)
                {
                    Console.WriteLine("creating {0} -> {1} T:{2} DT:{3}", tag.id, tag.name, tag.type, tag.dtype);
                    cmd_insert.Parameters["@1"].Value = tag.name;
                    cmd_insert.Parameters["@2"].Value = tag.id;
                    cmd_insert.Parameters["@3"].Value = tag.type;
                    cmd_insert.Parameters["@4"].Value = tag.dtype;
                    int affected = cmd_insert.ExecuteNonQuery();
                    sql_tagids[tag.id] = tag.id;
                }
                reader.Close();
            }
            
            string query_tpl = $"INSERT INTO [{FloatTable}] VALUES ";
            int batch_count = 0;
            string batch = "";

            foreach (DatFloatRecord val in dr.ReadFloatFile(floatfile_name))
            {
                batch_count++;
                DateTime datetime_sec = DateTime.ParseExact(new string(val.time_sec), "yyyyMMddHH:mm:ss", CultureInfo.InvariantCulture);
                batch += $"('{datetime_sec.ToString("yyyy-MM-dd HH:mm:ss")}',{val.milli},{sql_tagids[val.tagid]},{val.val},'{val.status}','{val.marker}'),";
                if (batch_count == Globals.BATCH_SIZE)
                {
                    string query = query_tpl + batch.Substring(0, batch.Length - 1);
                    SqlCommand cmd = new SqlCommand(query, conn);
                    int affected = cmd.ExecuteNonQuery();
                    batch_count = 0;
                    batch = "";
                }
            }
            if (batch_count > 0)
            {
                string query = query_tpl + batch.Substring(0, batch.Length - 1);
                SqlCommand cmd = new SqlCommand(query, conn);
                int affected = cmd.ExecuteNonQuery();
                batch_count = 0;
                batch = "";
            }
        }
    }
}
