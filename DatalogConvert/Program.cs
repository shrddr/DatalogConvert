using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatalogConvert
{
    public class Program
    {
        class TagTableRow
        {
            public string name;
            public Int16 index;
            public byte type;
            public Int16 datatype;
        }

        class FloatTableRow
        {
            public DateTime dt;
            public Int16 millitm;
            public Int16 tagindex;
            public double value;
            public char status;
            public char marker;
        }

        class TagInfo
        {
            public string name;
            public byte type;
            public Int16 datatype;
            public bool isFinalized;
            public double initialValue;
            public int lastRowId;
        }

        static byte[] makeTagHeader(Int32 rowcount)
        {
            MemoryStream memStream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(memStream);
            int yy = DateTime.Now.Year - 1900;
            int mm = DateTime.Now.Month;
            int dd = DateTime.Now.Day;
            writer.Write((byte)0x03);
            writer.Write((byte)yy);
            writer.Write((byte)mm);
            writer.Write((byte)dd);
            writer.Write(rowcount);
            writer.Write(0x010800A1);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Tagname".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000001);
            writer.Write(0x000000FF);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("TTagIndex".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000100);
            writer.Write(0x00000005);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("TagType".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000105);
            writer.Write(0x00000001);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("TagDataTyp".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000106);
            writer.Write(0x00000002);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write((byte)0x0D);
            return memStream.ToArray();
        }

        static byte[] makeFloatHeader(Int32 rowcount)
        {
            MemoryStream memStream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(memStream);
            int yy = DateTime.Now.Year - 1900;
            int mm = DateTime.Now.Month;
            int dd = DateTime.Now.Day;
            writer.Write((byte)0x03);
            writer.Write((byte)yy);
            writer.Write((byte)mm);
            writer.Write((byte)dd);
            writer.Write(rowcount);
            writer.Write(0x00270121);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Date".PadRight(11, '\0')));
            writer.Write('D');
            writer.Write(0x00000001);
            writer.Write(0x00000008);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Time".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000009);
            writer.Write(0x00000008);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Millitm".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000011);
            writer.Write(0x00000003);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("TagIndex".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000014);
            writer.Write(0x00000005);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Value".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000019);
            writer.Write(0x00000008);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Status".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000021);
            writer.Write(0x00000001);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Marker".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000022);
            writer.Write(0x00000001);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Internal".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000023);
            writer.Write(0x00000004);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write((byte)0x0D);
            return memStream.ToArray();
        }

        static byte[] makeStringHeader(Int32 rowcount)
        {
            MemoryStream memStream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(memStream);
            int yy = DateTime.Now.Year - 1900;
            int mm = DateTime.Now.Month;
            int dd = DateTime.Now.Day;
            writer.Write((byte)0x03);
            writer.Write((byte)yy);
            writer.Write((byte)mm);
            writer.Write((byte)dd);
            writer.Write(rowcount);
            writer.Write(0x00710121);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Date".PadRight(11, '\0')));
            writer.Write('D');
            writer.Write(0x00000001);
            writer.Write(0x00000008);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Time".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000009);
            writer.Write(0x00000008);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Millitm".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000011);
            writer.Write(0x00000003);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("TagIndex".PadRight(11, '\0')));
            writer.Write('N');
            writer.Write(0x00000014);
            writer.Write(0x00000005);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Value".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x00000019);
            writer.Write(0x00000052);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Status".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x0000006B);
            writer.Write(0x00000001);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Marker".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x0000006C);
            writer.Write(0x00000001);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(Encoding.ASCII.GetBytes("Internal".PadRight(11, '\0')));
            writer.Write('C');
            writer.Write(0x0000006D);
            writer.Write(0x00000004);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write(0x00000000);
            writer.Write((byte)0x0D);
            return memStream.ToArray();
        }

        static void Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine("Error: argument not found");
                Console.WriteLine("Usage: DatalogConvert ServerName CatalogName User Password [FloatTable] [TagTable] [StringTable]");
                Console.ReadLine();
                return;
            }

            string FloatTable = args.Length > 4 ? args[4] : "FloatTable";
            string TagTable = args.Length > 5 ? args[5] : "TagTable";
            string StringTable = args.Length > 6 ? args[6] : "StringTable";

            string connString = $"Data Source={args[0]};Initial Catalog={args[1]};Persist Security Info=True;User ID={args[2]};Password={args[3]}";
            Make(connString, FloatTable, TagTable, StringTable);

            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        public static void Make(string connString, string TablePrefix)
        {
            Make(connString, $"{TablePrefix}FloatTable", $"{TablePrefix}TagTable", $"{TablePrefix}StringTable");
        }

        public static void Make(string connString, string FloatTableName, string TagTableName, string StringTableName)
        {
            List<TagTableRow> TagTableRows = new List<TagTableRow>();
            List<DateTime> Dates = new List<DateTime>();

            using (SqlConnection conn = new SqlConnection(connString))
            {
                try
                {
                    conn.Open();
                    string sql = $"SELECT * FROM [{TagTableName}] ORDER BY TagIndex";
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        string tn = reader.GetString(0);
                        Int16 ti = reader.GetInt16(1);
                        byte tt = (byte)reader.GetInt16(2);
                        Int16 td = reader.GetInt16(3);
                        TagTableRows.Add(new TagTableRow() { name = tn, index = ti, type = tt, datatype = td });
                    }
                    reader.Close();
                    Console.WriteLine("Tags: " + TagTableRows.Count);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error reading tags: " + ex.Message);
                }

                try
                {
                    string sql = $"SELECT DISTINCT(CAST(DateAndTime AS DATE)) FROM [{FloatTableName}]";
                    SqlCommand cmd = new SqlCommand(sql, conn) { CommandTimeout = 600 };
                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        DateTime dt = reader.GetDateTime(0);
                        Dates.Add(dt);
                    }
                    reader.Close();
                    Console.WriteLine("Dates: " + Dates.Count);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error reading dates: " + ex.Message);
                }

                try
                {
                    foreach (DateTime currentDay in Dates)
                    {
                        List<FloatTableRow> FloatTableRows = new List<FloatTableRow>();
                        var TagData = new Dictionary<Int16, TagInfo>();

                        string sql = $"SELECT * FROM [{FloatTableName}] WHERE DateAndTime > @DT AND DateAndTime < DATEADD(day, 1, @DT) ORDER BY DateAndTime, Millitm";
                        SqlCommand cmd = new SqlCommand(sql, conn) { CommandTimeout = 600 };
                        cmd.Parameters.AddWithValue("DT", currentDay);
                        SqlDataReader reader = cmd.ExecuteReader();

                        // first scan: 
                        while (reader.Read())
                        {
                            DateTime dt = reader.GetDateTime(0);
                            Int16 mi = reader.GetInt16(1);
                            Int16 tagindex = reader.GetInt16(2);
                            double v = reader.GetDouble(3);
                            string str = reader.GetString(4);
                            char s = str[0];
                            str = reader.GetString(5);
                            char m = str[0];

                            if (!TagData.ContainsKey(tagindex))
                            {
                                TagTableRow t = TagTableRows.Find(row => row.index == tagindex);
                                // TODO: initialValue can be interpolated nicely between last record of previous day and first value of current day
                                TagData.Add(t.index, new TagInfo() {
                                    name = t.name,
                                    type = t.type,
                                    datatype = t.datatype,
                                    initialValue = v,
                                    lastRowId = -1 });
                                m = 'B';
                            }
                            FloatTableRows.Add(new FloatTableRow() {
                                dt = dt,
                                millitm = mi,
                                tagindex = tagindex,
                                value = v,
                                status = s,
                                marker = m });

                            TagData[tagindex].isFinalized = (m == 'E');
                        }
                        reader.Close();

                        // if last occurence of a tag is not marked with 'E', add it
                        // if it already has B marker, duplicate the row
                        foreach (short i in TagData.Keys)
                        {
                            TagInfo t = TagData[i];
                            if (t.isFinalized) continue;
                            var lastRow = FloatTableRows.Where(row => row.tagindex == i).Last();
                            if (lastRow.marker == 'B')
                                FloatTableRows.Add(new FloatTableRow() {
                                    dt = lastRow.dt,
                                    millitm = lastRow.millitm,
                                    tagindex = lastRow.tagindex,
                                    value = lastRow.value,
                                    status = lastRow.status,
                                    marker = 'E'
                                });
                            else
                                lastRow.marker = 'E';
                        }

                        string dayString = currentDay.ToString("yyyy MM dd");
                        Console.WriteLine(dayString + " values: " + FloatTableRows.Count);
                        string dayFloatFile = dayString + " 0000 (Float).DAT";
                        string dayTagFile = dayString + " 0000 (Tagname).DAT";
                        string dayStringFile = dayString + " 0000 (String).DAT";
                        byte[] floatHeader = makeFloatHeader(FloatTableRows.Count);
                        byte[] tagHeader = makeTagHeader(TagData.Count);
                        byte[] stringHeader = makeStringHeader(0);

                        using (BinaryWriter writer = new BinaryWriter(File.Open(dayFloatFile, FileMode.Create)))
                        {
                            writer.Write(floatHeader);

                            int currentRow = 0;
                            foreach (FloatTableRow f in FloatTableRows)
                            {
                                writer.Write((byte)0x20);
                                writer.Write(Encoding.ASCII.GetBytes(f.dt.ToString("yyyyMMdd")));
                                writer.Write(Encoding.ASCII.GetBytes(f.dt.ToString("HH:mm:ss")));
                                writer.Write(Encoding.ASCII.GetBytes(f.millitm.ToString().PadLeft(3, ' ')));
                                writer.Write(Encoding.ASCII.GetBytes(f.tagindex.ToString().PadLeft(5, ' ')));
                                writer.Write(f.value);
                                writer.Write(f.status);
                                writer.Write(f.marker);
                                writer.Write(TagData[f.tagindex].lastRowId);
                                TagData[f.tagindex].lastRowId = currentRow++;
                            } 
                        }
                        using (BinaryWriter writer = new BinaryWriter(File.Open(dayTagFile, FileMode.Create)))
                        {
                            writer.Write(tagHeader);
                            foreach (short i in TagData.Keys)
                            {
                                TagInfo t = TagData[i];
                                writer.Write((byte)0x20);
                                writer.Write(Encoding.ASCII.GetBytes(t.name.ToUpper().PadRight(255, ' ')));
                                writer.Write(Encoding.ASCII.GetBytes(i.ToString().PadLeft(5, ' ')));
                                writer.Write(Encoding.ASCII.GetBytes(t.type.ToString().PadLeft(1, ' ')));
                                writer.Write(Encoding.ASCII.GetBytes(t.datatype.ToString().PadLeft(2, ' ')));
                            }
                            writer.Write((byte)0x1A);
                        }
                        using (BinaryWriter writer = new BinaryWriter(File.Open(dayStringFile, FileMode.Create)))
                            writer.Write(stringHeader);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error reading values: " + ex.Message);
                }
            }
        }
    }
}