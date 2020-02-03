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
                string query = $"CREATE TABLE [{FloatTable}] ([DateAndTime] [datetime] NULL, [Millitm] [smallint] NULL, [TagIndex] [smallint] NULL, [Val] [float] NULL, [Status] [nvarchar] (1) NULL, [Marker] [nvarchar] (1) NULL)"; 
                SqlCommand cmd = new SqlCommand(query, conn);
                int affected = cmd.ExecuteNonQuery();
                res &= affected > 0;

                if (TableExists(conn, TagTable))
                {
                    DropTable(conn, TagTable);
                }
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
                Console.ReadLine();
                return;
            }

            string FloatTable = "FloatTable";
            string TagTable = "TagTable";
            string StringTable = "StringTable";
            if (args.Length > 4)
            {
                FloatTable = $"{args[4]}_FloatTable";
                TagTable = $"{args[4]}_TagTable";
                StringTable = $"{args[4]}_StringTable";
            }

            string connString = $"Data Source={args[0]};Initial Catalog={args[1]};Persist Security Info=True;User ID={args[2]};Password={args[3]}";

            SqlConnection conn = new SqlConnection(connString);
            conn.Open();
            CreateTables(conn, TagTable, FloatTable);

            string[] files = Directory.GetFiles(".", "* (Float).DAT");
            int converted = 0;
            foreach (var filename in files)
            {
                Console.WriteLine("converting {0}", filename);
                MakeSQL(filename, conn, TagTable, FloatTable);
                converted++;
            }

            conn.Close();
            Console.WriteLine("converted {0} files", converted);
            Console.ReadLine();
        }

        

        public static void MakeSQL(string float_filename, SqlConnection conn, string TagTable, string FloatTable)
        {
            Dictionary<int, int> sql_tagids = new Dictionary<int, int>();

            try
            {
                string query_select = $"SELECT [TagIndex],[TagType],[TagDataType] FROM [{TagTable}] WHERE TagName = @1";
                SqlCommand cmd_select = new SqlCommand(query_select, conn);
                cmd_select.Parameters.Add("@1", SqlDbType.NVarChar, 255);

                string query_insert = $"INSERT INTO [{TagTable}] VALUES (@1,@2,@3,@4)";
                SqlCommand cmd_insert = new SqlCommand(query_insert, conn);
                cmd_insert.Parameters.Add("@1", SqlDbType.NVarChar, 255);
                cmd_insert.Parameters.Add("@2", SqlDbType.SmallInt);
                cmd_insert.Parameters.Add("@3", SqlDbType.SmallInt);
                cmd_insert.Parameters.Add("@4", SqlDbType.SmallInt);

                string tagname_filename = float_filename.Replace(" (Float)", " (Tagname)");
                BinaryReader br = new BinaryReader(File.Open(tagname_filename, FileMode.Open));
                byte ver = br.ReadByte();
                int yy = br.ReadByte() + 1900;
                byte mm = br.ReadByte();
                byte dd = br.ReadByte();
                int rowcount = br.ReadInt32();
                Console.WriteLine("{0} tags", rowcount);
                br.BaseStream.Seek(0xA1, SeekOrigin.Begin);

                Dictionary<int, string> tagnames = new Dictionary<int, string>();
                for (int i = 0; i < rowcount; i++)
                {
                    br.BaseStream.Seek(1, SeekOrigin.Current);
                    string tagname = new string(br.ReadChars(255)).Trim();
                    int tagid = int.Parse(new string(br.ReadChars(5)));
                    int tagtype = int.Parse(new string(br.ReadChars(1)));
                    int tagdtype = int.Parse(new string(br.ReadChars(2)));

                    bool tag_exists = false;
                    cmd_select.Parameters["@1"].Value = tagname;
                    SqlDataReader reader = cmd_select.ExecuteReader();
                    if (reader.HasRows)
                    {
                        reader.Read();
                        Int16 ti = reader.GetInt16(0);
                        byte tt = (byte)reader.GetInt16(1);
                        Int16 td = reader.GetInt16(2);
                        if (tt == tagtype && td == tagdtype)
                        {
                            tag_exists = true;
                            sql_tagids[tagid] = ti;
                        }
                    }
                    reader.Close();
                    if (!tag_exists)
                    {
                        Console.WriteLine("creating {0} -> {1} T:{2} DT:{3}", tagid, tagname, tagtype, tagdtype);
                        cmd_insert.Parameters["@1"].Value = tagname;
                        cmd_insert.Parameters["@2"].Value = tagid;
                        cmd_insert.Parameters["@3"].Value = tagtype;
                        cmd_insert.Parameters["@4"].Value = tagdtype;
                        int affected = cmd_insert.ExecuteNonQuery();
                        sql_tagids[tagid] = tagid;
                    }
                    reader.Close();

                }
                br.BaseStream.Close();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error writing tags: " + ex.Message);
            }

            try
            {
                string query_tpl = $"INSERT INTO [{FloatTable}] VALUES ";
                
                //cmd.Parameters.Add("@1", SqlDbType.DateTime);
                //cmd.Parameters.Add("@2", SqlDbType.SmallInt);
                //cmd.Parameters.Add("@3", SqlDbType.SmallInt);
                //cmd.Parameters.Add("@4", SqlDbType.Float);
                //cmd.Parameters.Add("@5", SqlDbType.NVarChar, 1);
                //cmd.Parameters.Add("@6", SqlDbType.NVarChar, 1);

                BinaryReader br = new BinaryReader(File.Open(float_filename, FileMode.Open));
                byte ver = br.ReadByte();
                int yy = br.ReadByte() + 1900;
                byte mm = br.ReadByte();
                byte dd = br.ReadByte();
                int rowcount = br.ReadInt32();
                Console.WriteLine("{0} values", rowcount);

                br.BaseStream.Seek(0x121, SeekOrigin.Begin);

                int BATCH_SIZE = 1000;
                int batch_len = 0;
                string batch = "";

                DateTime last_progress_print = DateTime.Now;
                for (int i = 0; i < rowcount; i++)
                {
                    br.BaseStream.Seek(1, SeekOrigin.Current);
                    char[] time = br.ReadChars(16);
                    DateTime datetime = DateTime.ParseExact(new string(time), "yyyyMMddHH:mm:ss", CultureInfo.InvariantCulture);
                    Int16 milli = Int16.Parse(new string(br.ReadChars(3)));
                    Int16 tagid = Int16.Parse(new string(br.ReadChars(5)));
                    double val = br.ReadDouble();
                    char status = br.ReadChar();
                    char marker = br.ReadChar();
                    br.BaseStream.Seek(4, SeekOrigin.Current);

                    batch_len++;
                    batch += $"('{datetime.ToString("yyyy-MM-dd HH:mm:ss")}',{milli},{sql_tagids[tagid]},{val},'{status}','{marker}'),";
                    if (batch_len == BATCH_SIZE)
                    {
                        string query = query_tpl + batch.Substring(0, batch.Length-1);
                        SqlCommand cmd = new SqlCommand(query, conn);
                        int affected = cmd.ExecuteNonQuery();
                        batch_len = 0;
                        batch = "";
                    }

                    //cmd.Parameters["@1"].Value = datetime;
                    //cmd.Parameters["@2"].Value = milli;
                    //cmd.Parameters["@3"].Value = tagid;
                    //cmd.Parameters["@4"].Value = val;
                    //cmd.Parameters["@5"].Value = status;
                    //cmd.Parameters["@6"].Value = marker;

                    if ((DateTime.Now - last_progress_print).TotalSeconds > 3)
                    {
                        Console.WriteLine($"{100 * i / rowcount}%");
                        last_progress_print = DateTime.Now;
                    }
                }
                if (batch_len > 0)
                {
                    string query = query_tpl + batch.Substring(0, batch.Length - 1);
                    SqlCommand cmd = new SqlCommand(query, conn);
                    int affected = cmd.ExecuteNonQuery();
                    batch_len = 0;
                    batch = "";
                }
                br.BaseStream.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error writing values: " + ex.Message);
            }

        }
    }
}
