using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dat2fth
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Error: argument not found");
                Console.WriteLine("Usage: dat2fth PointPrefix [InputPattern]");
                return;
            }
            string tagname_prefix = args[0];

            string pattern = "./* (Float).DAT";
            if (args.Length > 1)
            {
                pattern = args[1];
            }

            string wildcard = Path.GetFileName(pattern);
            string relPath = pattern.Substring(0, pattern.Length - wildcard.Length);
            string fullPath = Path.GetFullPath(relPath);
            string[] files = Directory.GetFiles(fullPath, wildcard); // can lazy EnumerateFiles instead
            int converted = 0;

            using (StreamWriter outwriter = new StreamWriter(File.Open("out.csv", FileMode.Create)))
            {
                outwriter.Write("@mode edit, t\n");
                outwriter.Write("@table pisnap\n");
                outwriter.Write("@istr tag, time, value\n");
                foreach (var infilename in files)
                {
                    Console.WriteLine("converting {0}", Path.GetFileName(infilename));
                    MakeFTH(infilename, outwriter, tagname_prefix);
                    converted++;
                }
            }
            Console.WriteLine("converted {0} files", converted);
        }


        public static void MakeFTH(string float_filename, StreamWriter outwriter, string tagname_prefix)
        {
            Dictionary<int, string> tagnames = new Dictionary<int, string>();
            try
            {
                string tagname_filename = float_filename.Replace(" (Float)", " (Tagname)");
                BinaryReader br = new BinaryReader(File.Open(tagname_filename, FileMode.Open));
                byte ver = br.ReadByte();
                int yy = br.ReadByte() + 1900;
                byte mm = br.ReadByte();
                byte dd = br.ReadByte();
                int rowcount = br.ReadInt32();
                Console.WriteLine("{0} tags", rowcount);
                br.BaseStream.Seek(0xA1, SeekOrigin.Begin);

                for (int i = 0; i < rowcount; i++)
                {
                    br.BaseStream.Seek(1, SeekOrigin.Current);
                    string tagname = new string(br.ReadChars(255)).Trim();
                    int tagid = int.Parse(new string(br.ReadChars(5)));
                    int tagtype = int.Parse(new string(br.ReadChars(1)));
                    int tagdtype = int.Parse(new string(br.ReadChars(2)));
                    tagname = tagname_prefix + tagname.Replace('\\', '.');
                    tagnames[tagid] = tagname;
                }
                br.BaseStream.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error reading tags: " + ex.Message);
            }

            try
            {
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

                CultureInfo culture = new CultureInfo("en-US");
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
                    // A1HV074B,08-Aug-01 11:00:00,3659
                    // Historian doesn't want the milliseconds? WTF
                    // TODO: probably discard some of the values based on STATUS field
                    batch += $"{tagnames[tagid]},{datetime.ToString("dd-MMM-yy HH:mm:ss", culture)},{val}\n";
                    if (batch_len == BATCH_SIZE)
                    {
                        outwriter.Write(batch); // async?
                        batch_len = 0;
                        batch = "";
                    }

                    if ((DateTime.Now - last_progress_print).TotalSeconds > 3)
                    {
                        Console.WriteLine($"{100 * i / rowcount}%");
                        last_progress_print = DateTime.Now;
                    }
                }
                if (batch_len > 0)
                {
                    outwriter.Write(batch);
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
