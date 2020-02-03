using System;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using sql2dat;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System.Linq;

namespace UnitTests
{
    public class TheData
    {
        public TheData()
        {
            tags = new List<TagRow>();
            floats = new List<FloatRow>();
        }

        public class TagRow
        {
            public string tn;
            public int ti;
            public int tt;
            public int td;
        }

        public class FloatRow
        {
            public string date;
            public string time;
            public int millitm;
            public int tagindex;
            public double value;
            public string status;
            public string marker;
            public int intern;

            public override string ToString() { return $"{date} {time} {millitm} {tagindex} {status} {marker} {intern}"; }
        }

        public List<TagRow> tags;
        public List<FloatRow> floats;
    }

    [TestClass]
    public class test_sql2dat
    {
        string connString = $"Data Source=A20180213\\DATALOG;Initial Catalog=master;Persist Security Info=True;User ID=sa;Password=123qwe!@#";

        public static bool ExecBool(SqlConnection conn, string query)
        {
            SqlCommand cmd = new SqlCommand(query, conn);
            int affected = cmd.ExecuteNonQuery();
            return (affected != 0);
        }

        public static int ExecInt(SqlConnection conn, string query)
        {
            SqlCommand cmd = new SqlCommand(query, conn);
            int result = (int)cmd.ExecuteScalar();
            return result;
        }

        public void CreateTables(string TablePrefix, TheData data)
        {
            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();

                ExecBool(conn, $"IF OBJECT_ID('{TablePrefix}TagTable', 'U') IS NOT NULL DROP TABLE [{TablePrefix}TagTable]");
                ExecBool(conn, $"CREATE TABLE [{TablePrefix}TagTable] ([TagName] [nvarchar](255) NULL, [TagIndex] [smallint] NULL, [TagType] [smallint] NULL, [TagDataType] [smallint] NULL)");
                foreach (var t in data.tags)
                    ExecBool(conn, $"INSERT INTO [{TablePrefix}TagTable] VALUES ('{t.tn}', {t.ti}, {t.tt}, {t.td})");

                ExecBool(conn, $"IF OBJECT_ID('{TablePrefix}FloatTable', 'U') IS NOT NULL DROP TABLE [{TablePrefix}FloatTable]");
                ExecBool(conn, $"CREATE TABLE [{TablePrefix}FloatTable] ([DateAndTime] [datetime] NULL, [Millitm] [smallint] NULL, [TagIndex] [smallint] NULL, [Val] [float] NULL, [Status] [nvarchar] (1) NULL, [Marker] [nvarchar] (1) NULL)");
                foreach (var f in data.floats)
                    ExecBool(conn, $"INSERT INTO [{TablePrefix}FloatTable] VALUES ('{f.date} {f.time}', {f.millitm}, {f.tagindex}, {f.value}, '{f.status}', '{f.marker}')");
            }
        }

        public static Stream StringStream(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        public TheData readCSV(string inputData)
        {
            TheData data = new TheData();
            using (var stream = StringStream(inputData))
                using (TextFieldParser csvParser = new TextFieldParser(stream))
                {
                    csvParser.CommentTokens = new string[] { ";" };
                    csvParser.SetDelimiters(new string[] { "," });

                    while (!csvParser.EndOfData)
                    {
                        string[] fields = csvParser.ReadFields();
                        if (fields.Length == 5)
                        {
                            TheData.TagRow t = new TheData.TagRow();
                            t.tn = fields[0];
                            t.ti = int.Parse(fields[1]);
                            t.tt = int.Parse(fields[2]);
                            t.td = int.Parse(fields[3]);
                            data.tags.Add(t);
                        }
                        else
                        {
                            TheData.FloatRow f = new TheData.FloatRow();
                            f.date = fields[0];
                            f.time = fields[1];
                            f.millitm = int.Parse(fields[2]);
                            f.tagindex = int.Parse(fields[3]);
                            f.value = double.Parse(fields[4]);
                            f.status = fields[5];
                            if (String.IsNullOrEmpty(f.status))
                                f.status = " ";
                            f.marker = fields[6];
                            if (String.IsNullOrEmpty(f.marker))
                                f.marker = " ";
                            f.intern = int.Parse(fields[7]);
                            data.floats.Add(f);
                        }
                    }
                }
            return data;
        }

        public TheData readCSVfile(string fileName)
        {
            TheData data = new TheData();
            using (TextFieldParser csvParser = new TextFieldParser(fileName))
            {
                csvParser.CommentTokens = new string[] { ";" };
                csvParser.SetDelimiters(new string[] { "," });

                while (!csvParser.EndOfData)
                {
                    string[] fields = csvParser.ReadFields();
                    if (fields.Length == 5)
                    {
                        TheData.TagRow t = new TheData.TagRow();
                        t.tn = fields[0];
                        t.ti = int.Parse(fields[1]);
                        t.tt = int.Parse(fields[2]);
                        t.td = int.Parse(fields[3]);
                        data.tags.Add(t);
                    }
                    else
                    {
                        TheData.FloatRow f = new TheData.FloatRow();
                        f.date = fields[0];
                        f.time = fields[1];
                        f.millitm = int.Parse(fields[2]);
                        f.tagindex = int.Parse(fields[3]);
                        f.value = double.Parse(fields[4]);
                        f.status = fields[5];
                        if (String.IsNullOrEmpty(f.status))
                            f.status = " ";
                        f.marker = fields[6];
                        if (String.IsNullOrEmpty(f.marker))
                            f.marker = " ";
                        f.intern = int.Parse(fields[7]);
                        data.floats.Add(f);
                    }
                }
            }
            return data;
        }

        public TheData readDAT(string tagsFileName, string floatsFileName)
        {
            TheData result = new TheData();
            using (BinaryReader b = new BinaryReader(File.Open(tagsFileName, FileMode.Open)))
            {
                int pos = 0xA1;
                b.BaseStream.Seek(pos, SeekOrigin.Begin);
                int length = (int)b.BaseStream.Length - 1; //tagfile has a 0x1A end marker

                while (pos < length)
                {
                    TheData.TagRow t = new TheData.TagRow();
                    b.ReadByte(); // 0x20
                    char[] c = b.ReadChars(255);
                    t.tn = new string(c);
                    c = b.ReadChars(5);
                    t.ti = int.Parse(new string(c));
                    t.tt = int.Parse(new string(b.ReadChars(1)));
                    t.td = int.Parse(new string(b.ReadChars(2)));
                    result.tags.Add(t);
                    pos += 264;
                }
            }
            using (BinaryReader b = new BinaryReader(File.Open(floatsFileName, FileMode.Open)))
            {
                int pos = 0x121;
                b.BaseStream.Seek(pos, SeekOrigin.Begin);
                int length = (int)b.BaseStream.Length;

                while (pos < length)
                {
                    TheData.FloatRow f = new TheData.FloatRow();
                    b.ReadByte(); // 0x20
                    f.date = new string(b.ReadChars(8));
                    f.time = new string(b.ReadChars(8));
                    f.millitm = int.Parse(new string(b.ReadChars(3)));
                    f.tagindex = int.Parse(new string(b.ReadChars(5)));
                    f.value = b.ReadDouble();
                    f.status = b.ReadChar().ToString();
                    f.marker = b.ReadChar().ToString();
                    f.intern = b.ReadInt32();
                    result.floats.Add(f);
                    pos += 39;
                }
            }
            return result;
        }

        public void PrettyPrint(TheData inData, TheData outData)
        {
            Console.WriteLine($"{"--------------- inData ----------------",-40} |\t {"--------------- outData ----------------",-40}");
            int fc = inData.floats.Count >= outData.floats.Count ? inData.floats.Count : outData.floats.Count;
            for (int i = 0; i < fc; i++)
            {
                string s1 = i < inData.floats.Count ? inData.floats[i].ToString() : "NULL NULL NULL NULL";
                string s2 = i < outData.floats.Count ? outData.floats[i].ToString() : "NULL NULL NULL NULL";
                Console.WriteLine($"{s1,-40} |\t {s2,-40}");
            }
        }

        [TestMethod]
        public void Test_Add_Begins_and_Ends()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/12/2018,00:01:01,711,    0,       3.95120931, , ,         0, ; the program should add B and E markers
11/12/2018,00:01:01,865,    1,       4.02576590, , ,         1,
11/12/2018,00:02:01,980,    0,       7.53587818, , ,         2,
11/12/2018,00:02:01,980,    1,       5.03494072, , ,         3,");
            CreateTables("no_bs_and_es_", inData);
            Program.MakeDatFiles(connString, "no_bs_and_es_");

            var outData = readDAT("2018 11 12 0000 (Tagname).DAT", "2018 11 12 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<string> actual = outData.floats.Select(f => f.marker).ToList();
            CollectionAssert.AreEqual(new string[] { "B", "B", "E", "E" }, actual);
        }

        [TestMethod]
        public void Test_Dont_Add_Begins_and_Ends()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
AI\EIA15,    2,2, 1,
AI\EIA177,    3,2, 1,
AI\EIA185,    4,2, 1,
AI\EIA191,    5,2, 1,
AI\EIA243,    6,2, 1,
AI\EIA246,    7,2, 1,
AI\EIA251,    8,2, 1,
AI\BSK_TARGET,    9,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/13/2018,00:00:01, 55,    0,       7.53587818, ,B,        -1, ; don't add B and E if they are present
11/13/2018,00:00:01, 55,    1,       3.87665296, ,B,        -1,
11/13/2018,00:00:01, 55,    2,      27.11154175, ,B,        -1,
11/13/2018,00:00:01, 55,    3,       2.48232007, ,B,        -1,
11/13/2018,00:00:01, 55,    4,       1.80653179, ,B,        -1,
11/13/2018,00:00:01, 55,    5,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    6,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    7,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    8,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    9,     750.00000000, ,B,        -1,
11/13/2018,00:02:55,980,    0,       7.53587818, ,E,        0,
11/13/2018,00:02:55,980,    1,       5.03494072, ,E,        1,
11/13/2018,00:02:55,980,    2,      32.55123901, ,E,        2,
11/13/2018,00:02:55,980,    3,       2.48232007, ,E,        3,
11/13/2018,00:02:55,980,    4,       1.80653179, ,E,        4,
11/13/2018,00:02:55,980,    5,       0.00000000, ,E,        5,
11/13/2018,00:02:55,980,    6,       0.00000000, ,E,        6,
11/13/2018,00:02:55,980,    7,       0.00000000, ,E,        7,
11/13/2018,00:02:55,980,    8,       0.00000000, ,E,        8,
11/13/2018,00:02:55,980,    9,     750.00000000, ,E,        9,");
            CreateTables("bs_and_es_", inData);
            Program.MakeDatFiles(connString, "bs_and_es_");

            var outData = readDAT("2018 11 13 0000 (Tagname).DAT", "2018 11 13 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<string> actual = outData.floats.Select(f => f.marker).ToList();
            List<string> expected = new List<string>();
            for (int i = 0; i < 10; i++) expected.Add("B");
            for (int i = 0; i < 10; i++) expected.Add("E");
            CollectionAssert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void Test_Begin_and_End_On_Different_Days()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
AI\EIA15,    2,2, 1,
AI\EIA177,    3,2, 1,
AI\EIA185,    4,2, 1,
AI\EIA191,    5,2, 1,
AI\EIA243,    6,2, 1,
AI\EIA246,    7,2, 1,
AI\EIA251,    8,2, 1,
AI\BSK_TARGET,    9,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/13/2018,23:59:01, 55,    0,       7.53587818, ,B,        -1, 
11/13/2018,23:59:01, 55,    1,       3.87665296, ,B,        -1,
11/13/2018,23:59:01, 55,    2,      27.11154175, ,B,        -1,
11/13/2018,23:59:01, 55,    3,       2.48232007, ,B,        -1,
11/13/2018,23:59:01, 55,    4,       1.80653179, ,B,        -1,
11/13/2018,23:59:01, 55,    5,       0.00000000, ,B,        -1,
11/13/2018,23:59:01, 55,    6,       0.00000000, ,B,        -1,
11/13/2018,23:59:01, 55,    7,       0.00000000, ,B,        -1,
11/13/2018,23:59:01, 55,    8,       0.00000000, ,B,        -1,
11/13/2018,23:59:01, 55,    9,     750.00000000, ,B,        -1,
11/13/2018,23:59:55,980,    0,       7.53587818, , ,        0,
11/13/2018,23:59:55,980,    1,       5.03494072, , ,        1,
11/13/2018,23:59:55,980,    2,      32.55123901, , ,        2,
11/13/2018,23:59:55,980,    3,       2.48232007, , ,        3, ; insert a full E snapshot here.
11/14/2018,00:02:55,980,    4,       1.80653179, , ,        4, ; then start a new file, which
11/14/2018,00:02:55,980,    5,       0.00000000, , ,        5, ; begins with a full B snapshot
11/14/2018,00:02:55,980,    6,       0.00000000, , ,        6,
11/14/2018,00:02:55,980,    7,       0.00000000, , ,        7,
11/14/2018,00:02:55,980,    8,       0.00000000, , ,        8,
11/14/2018,00:02:55,980,    9,     750.00000000, , ,        9,
11/14/2018,00:09:55,980,    0,       7.53587818, ,E,        10,
11/14/2018,00:09:55,980,    1,       5.03494072, ,E,        11,
11/14/2018,00:09:55,980,    2,      32.55123901, ,E,        12,
11/14/2018,00:09:55,980,    3,       2.48232007, ,E,        13,
11/14/2018,00:09:55,980,    4,       1.80653179, ,E,        14,
11/14/2018,00:09:55,980,    5,       0.00000000, ,E,        15,
11/14/2018,00:02:55,980,    6,       0.00000000, ,E,        16,
11/14/2018,00:09:55,980,    7,       0.00000000, ,E,        17,
11/14/2018,00:09:55,980,    8,       0.00000000, ,E,        18,
11/14/2018,00:09:55,980,    9,     750.00000000, ,E,        19,");
            CreateTables("b_e_diff_", inData);
            Program.MakeDatFiles(connString, "b_e_diff_");

            var data1 = readDAT("2018 11 13 0000 (Tagname).DAT", "2018 11 13 0000 (Float).DAT");
            var Day1_B_count = data1.floats.Where(f => f.marker == "B").Count();
            var Day1_E_count = data1.floats.Where(f => f.marker == "E").Count();

            var data2 = readDAT("2018 11 14 0000 (Tagname).DAT", "2018 11 14 0000 (Float).DAT");
            var Day2_B_count = data2.floats.Where(f => f.marker == "B").Count();
            var Day2_E_count = data2.floats.Where(f => f.marker == "E").Count();

            PrettyPrint(data1, data2);

            Assert.AreEqual(10, Day1_B_count);
            Assert.AreEqual(10, Day1_E_count);
            Assert.AreEqual(10, Day2_B_count);
            Assert.AreEqual(10, Day2_E_count);
        }

        [TestMethod]
        public void Test_10tags_2changing()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
AI\EIA15,    2,2, 1,
AI\EIA177,    3,2, 1,
AI\EIA185,    4,2, 1,
AI\EIA191,    5,2, 1,
AI\EIA243,    6,2, 1,
AI\EIA246,    7,2, 1,
AI\EIA251,    8,2, 1,
AI\BSK_TARGET,    9,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/13/2018,00:00:01, 55,    0,       7.53587818, ,B,        -1,
11/13/2018,00:00:01, 55,    1,       3.87665296, ,B,        -1,
11/13/2018,00:00:01, 55,    2,      27.11154175, ,B,        -1,
11/13/2018,00:00:01, 55,    3,       2.48232007, ,B,        -1,
11/13/2018,00:00:01, 55,    4,       1.80653179, ,B,        -1,
11/13/2018,00:00:01, 55,    5,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    6,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    7,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    8,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    9,     750.00000000, ,B,        -1,
;
11/13/2018,00:00:12,711,    1,       3.95120931, , ,         1,
11/13/2018,00:00:23,865,    1,       4.02576590, , ,        10,
;
11/13/2018,00:00:31,684,    0,       7.53587818, , ,         0,
11/13/2018,00:00:31,684,    1,       4.07502604, , ,        11,
11/13/2018,00:00:31,684,    2,      28.04316902, , ,         2,
11/13/2018,00:00:31,684,    3,       2.48232007, , ,         3,
11/13/2018,00:00:31,684,    4,       1.80653179, , ,         4,
11/13/2018,00:00:31,684,    5,       0.00000000, , ,         5,
11/13/2018,00:00:31,684,    6,       0.00000000, , ,         6,
11/13/2018,00:00:31,684,    7,       0.00000000, , ,         7,
11/13/2018,00:00:31,684,    8,       0.00000000, , ,         8,
11/13/2018,00:00:31,684,    9,     750.00000000, , ,         9,
;
11/13/2018,00:00:42,117,    1,       4.14958286, , ,        13,
11/13/2018,00:00:53,270,    1,       4.22280788, , ,        22,
11/13/2018,00:01:03,410,    2,      29.06233025, , ,        14,
11/13/2018,00:01:05,438,    1,       4.30002689, , ,        23,
11/13/2018,00:01:16,592,    1,       4.37458324, , ,        25,
11/13/2018,00:01:27,731,    1,       4.44780827, , ,        26,
;
11/13/2018,00:01:31,677,    0,       7.53587818, , ,        12,
11/13/2018,00:01:31,677,    1,       4.47443581, , ,        27,
11/13/2018,00:01:31,677,    2,      29.91892624, , ,        24,
11/13/2018,00:01:31,677,    3,       2.48232007, , ,        15,
11/13/2018,00:01:31,677,    4,       1.80653179, , ,        16,
11/13/2018,00:01:31,677,    5,       0.00000000, , ,        17,
11/13/2018,00:01:31,677,    6,       0.00000000, , ,        18,
11/13/2018,00:01:31,677,    7,       0.00000000, , ,        19,
11/13/2018,00:01:31,677,    8,       0.00000000, , ,        20,
11/13/2018,00:01:31,677,    9,     750.00000000, , ,        21,
;
11/13/2018,00:01:42,941,    1,       4.55165482, , ,        29,
11/13/2018,00:01:54, 96,    1,       4.62354851, , ,        38,
11/13/2018,00:02:03,221,    2,      30.93183708, , ,        30,
11/13/2018,00:02:04,234,    1,       4.69677401, , ,        39,
11/13/2018,00:02:15,390,    1,       4.76999903, , ,        41,
11/13/2018,00:02:27,557,    1,       4.84721804, , ,        42,
;
11/13/2018,00:02:31,704,    0,       7.53587818, , ,        28,
11/13/2018,00:02:31,704,    1,       4.87517691, , ,        43,
11/13/2018,00:02:31,704,    2,      31.80093765, , ,        40,
11/13/2018,00:02:31,704,    3,       2.48232007, , ,        31,
11/13/2018,00:02:31,704,    4,       1.80653179, , ,        32,
11/13/2018,00:02:31,704,    5,       0.00000000, , ,        33,
11/13/2018,00:02:31,704,    6,       0.00000000, , ,        34,
11/13/2018,00:02:31,704,    7,       0.00000000, , ,        35,
11/13/2018,00:02:31,704,    8,       0.00000000, , ,        36,
11/13/2018,00:02:31,704,    9,     750.00000000, , ,        37,
;
11/13/2018,00:02:42,752,    1,       4.94840193, , ,        45,
11/13/2018,00:02:53,906,    1,       5.02162695, , ,        54,
;
11/13/2018,00:02:55,980,    0,       7.53587818, ,E,        44,
11/13/2018,00:02:55,980,    1,       5.03494072, ,E,        55,
11/13/2018,00:02:55,980,    2,      32.55123901, ,E,        46,
11/13/2018,00:02:55,980,    3,       2.48232007, ,E,        47,
11/13/2018,00:02:55,980,    4,       1.80653179, ,E,        48,
11/13/2018,00:02:55,980,    5,       0.00000000, ,E,        49,
11/13/2018,00:02:55,980,    6,       0.00000000, ,E,        50,
11/13/2018,00:02:55,980,    7,       0.00000000, ,E,        51,
11/13/2018,00:02:55,980,    8,       0.00000000, ,E,        52,
11/13/2018,00:02:55,980,    9,     750.00000000, ,E,        53,
");
            CreateTables("10tags_2changing_", inData);
            Program.MakeDatFiles(connString, "10tags_2changing_");

            var outData = readDAT("2018 11 13 0000 (Tagname).DAT", "2018 11 13 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<int> expected = inData.floats.Select(f => f.intern).ToList();
            List<int> actual = outData.floats.Select(f => f.intern).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void Test_10tags_3changing()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
AI\EIA15,    2,2, 1,
AI\EIA177,    3,2, 1,
AI\EIA185,    4,2, 1,
AI\EIA191,    5,2, 1,
AI\EIA243,    6,2, 1,
AI\EIA246,    7,2, 1,
AI\EIA251,    8,2, 1,
AI\BSK_TARGET,    9,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/15/2018,22:05:11,130,    0,       7.53587818, ,B,        -1,
11/15/2018,22:05:11,130,    1,       4.23345852,S,B,        -1,
11/15/2018,22:05:11,130,    2,      87.78224182, ,B,        -1,
11/15/2018,22:05:11,130,    3,       2.70355415, ,B,        -1,
11/15/2018,22:05:11,130,    4,       1.80653179, ,B,        -1,
11/15/2018,22:05:11,130,    5,       0.00000000,S,B,        -1,
11/15/2018,22:05:11,130,    6,       0.00000000,S,B,        -1,
11/15/2018,22:05:11,130,    7,       0.00000000,S,B,        -1,
11/15/2018,22:05:11,130,    8,       0.00000000,S,B,        -1,
11/15/2018,22:05:11,130,    9,     750.00000000,S,B,        -1,
;
11/15/2018,22:05:11,838,    9,     750.00000000, , ,         9,
11/15/2018,22:05:11,838,    6,       0.00000000, , ,         6,
11/15/2018,22:05:11,838,    5,       0.00000000, , ,         5,
11/15/2018,22:05:11,838,    7,       0.00000000, , ,         7,
11/15/2018,22:05:11,838,    8,       0.00000000, , ,         8,
;
11/15/2018,22:05:11,838,    1,       1.94619894, , ,         1,
11/15/2018,22:05:21,978,    2,      88.81357574, , ,         2,
11/15/2018,22:05:28, 62,    3,       2.80979633, , ,         3,
11/15/2018,22:05:33,131,    2,      89.84491730, , ,        16,
11/15/2018,22:05:44,286,    1,       2.01722765, , ,        15,
11/15/2018,22:05:45,300,    3,       2.91353893, , ,        17,
11/15/2018,22:05:45,300,    2,      90.93251038, , ,        18,
11/15/2018,22:05:56,454,    2,      91.96385193, , ,        21,
11/15/2018,22:06:01,524,    3,       3.01478148, , ,        20,
11/15/2018,22:06:06,594,    2,      93.01393890, , ,        22,
;
11/15/2018,22:06:11,133,    0,       7.53587818, , ,         0,
11/15/2018,22:06:11,133,    1,       2.07671404, , ,        19,
11/15/2018,22:06:11,133,    2,      93.38897705, , ,        24,
11/15/2018,22:06:11,133,    3,       3.07727695, , ,        23,
11/15/2018,22:06:11,133,    4,       1.80653179, , ,         4,
11/15/2018,22:06:11,133,    5,       0.00000000, , ,        12,
11/15/2018,22:06:11,133,    6,       0.00000000, , ,        11,
11/15/2018,22:06:11,133,    7,       0.00000000, , ,        13,
11/15/2018,22:06:11,133,    8,       0.00000000, , ,        14,
11/15/2018,22:06:11,133,    9,     750.00000000, , ,        10,
;
11/15/2018,22:06:22,802,    2,      94.49532318, , ,        27,
11/15/2018,22:06:27,873,    3,       3.18476915, , ,        28,
11/15/2018,22:06:33,957,    2,      95.56416321, , ,        35,
11/15/2018,22:06:43, 83,    1,       2.14818668, , ,        26,
11/15/2018,22:06:44, 97,    3,       3.28601170, , ,        36,
11/15/2018,22:06:45,111,    2,      96.61425018, , ,        37,
11/15/2018,22:06:57,278,    2,      97.70185089, , ,        40,
11/15/2018,22:07:01,334,    3,       3.38975430, , ,        39,
11/15/2018,22:07:08,433,    2,      98.73318481, , ,        41,
;
11/15/2018,22:07:11,182,    0,       7.53587818, , ,        25,
11/15/2018,22:07:11,182,    1,       2.21078086, , ,        38,
11/15/2018,22:07:11,182,    2,      99.01445770, , ,        43,
11/15/2018,22:07:11,182,    3,       3.45224977, , ,        42,
11/15/2018,22:07:11,182,    4,       1.80653179, , ,        29,
11/15/2018,22:07:11,182,    5,       0.00000000, , ,        30,
11/15/2018,22:07:11,182,    6,       0.00000000, , ,        31,
11/15/2018,22:07:11,182,    7,       0.00000000, , ,        32,
11/15/2018,22:07:11,182,    8,       0.00000000, , ,        33,
11/15/2018,22:07:11,182,    9,     750.00000000, , ,        34,
;
11/15/2018,22:07:22,629,    2,       0.03749728, , ,        46,
11/15/2018,22:07:27,683,    3,       3.55849218, , ,        47,
;
11/15/2018,22:07:28,557,    0,       7.53587818, ,E,        44,
11/15/2018,22:07:28,557,    1,       2.24762702, ,E,        45,
11/15/2018,22:07:28,557,    2,       0.59995651, ,E,        54,
11/15/2018,22:07:28,557,    3,       3.55849218, ,E,        55,
11/15/2018,22:07:28,557,    4,       1.80653179, ,E,        48,
11/15/2018,22:07:28,557,    5,       0.00000000, ,E,        49,
11/15/2018,22:07:28,557,    6,       0.00000000, ,E,        50,
11/15/2018,22:07:28,557,    7,       0.00000000, ,E,        51,
11/15/2018,22:07:28,557,    8,       0.00000000, ,E,        52,
11/15/2018,22:07:28,557,    9,     750.00000000, ,E,        53,
");
            CreateTables("10tags_3changing_", inData);
            Program.MakeDatFiles(connString, "10tags_3changing_");

            var outData = readDAT("2018 11 15 0000 (Tagname).DAT", "2018 11 15 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<int> expected = inData.floats.Select(f => f.intern).ToList();
            List<int> actual = outData.floats.Select(f => f.intern).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void Test_Internal0_Simple_Increment()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/12/2018,00:00:01, 55,    0,       7.53587818, ,B,        -1,
11/12/2018,00:00:01, 55,    1,       3.87665296, ,B,        -1,
11/12/2018,00:01:01,711,    0,       3.95120931, , ,         0, ; looks like a simple increment right?
11/12/2018,00:01:01,865,    1,       4.02576590, , ,         1,
11/12/2018,00:02:01,980,    0,       7.53587818, ,E,         2,
11/12/2018,00:02:01,980,    1,       5.03494072, ,E,         3,");
            CreateTables("simplest_", inData);
            Program.MakeDatFiles(connString, "simplest_");

            var outData = readDAT("2018 11 12 0000 (Tagname).DAT", "2018 11 12 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<int> expected = inData.floats.Select(f => f.intern).ToList();
            List<int> actual = outData.floats.Select(f => f.intern).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void Test_Internal1_Heartbeat_And_Onchange_Mix()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
AI\EIA15,    2,2, 1,
AI\EIA177,    3,2, 1,
AI\EIA185,    4,2, 1,
AI\EIA191,    5,2, 1,
AI\EIA243,    6,2, 1,
AI\EIA246,    7,2, 1,
AI\EIA251,    8,2, 1,
AI\BSK_TARGET,    9,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/13/2018,00:00:01, 55,    0,      90.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    1,      70.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    2,      50.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    3,      30.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    4,      10.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    5,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    6,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    7,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    8,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    9,       0.00000000, ,B,        -1,
;
11/13/2018,00:00:31,684,    1,      70.00000000, , ,         1, 
11/13/2018,00:00:41,684,    1,      70.00000000, , ,        10,
;
11/13/2018,00:01:55,980,    0,      90.00000000, , ,        0,
11/13/2018,00:01:55,980,    1,      70.00000000, , ,        11,
11/13/2018,00:01:55,980,    2,      50.00000000, , ,        2,
11/13/2018,00:01:55,980,    3,      30.00000000, , ,        3,
11/13/2018,00:01:55,980,    4,      10.00000000, , ,        4,
11/13/2018,00:01:55,980,    5,       0.00000000, , ,        5,
11/13/2018,00:01:55,980,    6,       0.00000000, , ,        6,
11/13/2018,00:01:55,980,    7,       0.00000000, , ,        7,
11/13/2018,00:01:55,980,    8,       0.00000000, , ,        8,
11/13/2018,00:01:55,980,    9,       0.00000000, , ,        9,
;
11/13/2018,00:02:31,684,    1,      70.00000000, , ,        13,
11/13/2018,00:02:31,684,    2,      70.00000000, , ,        14,
11/13/2018,00:02:41,684,    1,      50.00000000, , ,        22,
11/13/2018,00:02:41,684,    2,      70.00000000, , ,        23,
;
11/13/2018,00:03:55,980,    0,      90.00000000, ,E,        12,
11/13/2018,00:09:55,980,    1,      70.00000000, ,E,        24,
11/13/2018,00:09:55,980,    2,      50.00000000, ,E,        25,
11/13/2018,00:09:55,980,    3,      30.00000000, ,E,        15,
11/13/2018,00:09:55,980,    4,      10.00000000, ,E,        16,
11/13/2018,00:09:55,980,    5,       0.00000000, ,E,        17,
11/13/2018,00:09:55,980,    6,       0.00000000, ,E,        18,
11/13/2018,00:09:55,980,    7,       0.00000000, ,E,        19,
11/13/2018,00:09:55,980,    8,       0.00000000, ,E,        20,
11/13/2018,00:09:55,980,    9,       0.00000000, ,E,        21,");
            CreateTables("internal1_heartbeat_and_onchange_mix_", inData);
            Program.MakeDatFiles(connString, "internal1_heartbeat_and_onchange_mix_");

            var outData = readDAT("2018 11 13 0000 (Tagname).DAT", "2018 11 13 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<int> expected = inData.floats.Select(f => f.intern).ToList();
            List<int> actual = outData.floats.Select(f => f.intern).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void Test_Internal2_Weird_Numbering()
        {
            var inData = readCSV(@";Tagname,TTagIndex,TagType,TagDataTyp
AI\EIA1_1,    0,2, 1,
AI\EIA1_2,    1,2, 1,
AI\EIA15,    2,2, 1,
AI\EIA177,    3,2, 1,
AI\EIA185,    4,2, 1,
AI\EIA191,    5,2, 1,
AI\EIA243,    6,2, 1,
AI\EIA246,    7,2, 1,
AI\EIA251,    8,2, 1,
AI\BSK_TARGET,    9,2, 1,
;Date,Time,Millitm,TagIndex,Value,Status,Marker,Internal
11/13/2018,00:00:01, 55,    0,      90.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    1,      70.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    2,      50.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    3,      30.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    4,      10.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    5,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    6,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    7,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    8,       0.00000000, ,B,        -1,
11/13/2018,00:00:01, 55,    9,       0.00000000, ,B,        -1,
;
11/13/2018,00:00:31,684,    1,      70.00000000, , ,         1,
11/13/2018,00:00:41,684,    1,      70.00000000, , ,        10,
;
11/13/2018,00:01:55,980,    0,      90.00000000, , ,        0,
11/13/2018,00:01:55,980,    1,      70.00000000, , ,        11,
11/13/2018,00:01:55,980,    2,      50.00000000, , ,        2,
11/13/2018,00:01:55,980,    3,      30.00000000, , ,        3,
11/13/2018,00:01:55,980,    4,      10.00000000, , ,        4,
11/13/2018,00:01:55,980,    5,       0.00000000, , ,        5,
11/13/2018,00:01:55,980,    6,       0.00000000, , ,        6,
11/13/2018,00:01:55,980,    7,       0.00000000, , ,        7,
11/13/2018,00:01:55,980,    8,       0.00000000, , ,        8,
11/13/2018,00:01:55,980,    9,       0.00000000, , ,        9,
;
11/13/2018,00:02:31,684,    1,      70.00000000, , ,        13,
11/13/2018,00:02:41,684,    1,      70.00000000, , ,        22,
11/13/2018,00:02:41,684,    2,      50.00000000, , ,        14,
11/13/2018,00:02:51,684,    1,      70.00000000, , ,        23,
;
11/13/2018,00:09:55,980,    0,      90.00000000, ,E,        12,
11/13/2018,00:09:55,980,    1,      70.00000000, ,E,        25,
11/13/2018,00:09:55,980,    2,      50.00000000, ,E,        24,
11/13/2018,00:09:55,980,    3,      30.00000000, ,E,        15,
11/13/2018,00:09:55,980,    4,      10.00000000, ,E,        16,
11/13/2018,00:09:55,980,    5,       0.00000000, ,E,        17,
11/13/2018,00:09:55,980,    6,       0.00000000, ,E,        18,
11/13/2018,00:09:55,980,    7,       0.00000000, ,E,        19,
11/13/2018,00:09:55,980,    8,       0.00000000, ,E,        20,
11/13/2018,00:09:55,980,    9,       0.00000000, ,E,        21,");
            CreateTables("internal2_weird_numbering_", inData);
            Program.MakeDatFiles(connString, "internal2_weird_numbering_");

            var outData = readDAT("2018 11 13 0000 (Tagname).DAT", "2018 11 13 0000 (Float).DAT");
            PrettyPrint(inData, outData);

            List<int> expected = inData.floats.Select(f => f.intern).ToList();
            List<int> actual = outData.floats.Select(f => f.intern).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }
    }
}
