using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace libDAT
{
    public class DatTagRecord
    {
        public string name;
        public int id;
        public int type;
        public int dtype;

        public DatTagRecord(BinaryReader br)
        {
            br.BaseStream.Seek(1, SeekOrigin.Current);
            name = new string(br.ReadChars(255)).Trim();
            id = int.Parse(new string(br.ReadChars(5)));
            type = int.Parse(new string(br.ReadChars(1)));
            dtype = int.Parse(new string(br.ReadChars(2)));
        }
    }

    public class DatFloatRecord
    {
        public char[] time_sec;
        public Int16 milli;
        public DateTime datetime;
        public Int16 tagid;
        public double val;
        public char status;
        public char marker;
        public bool IsValid { get; private set; } = true;

        public DatFloatRecord(BinaryReader br)
        {
            br.BaseStream.Seek(1, SeekOrigin.Current);
            time_sec = br.ReadChars(16);

            try
            {
                string dateString = new string(time_sec);
                datetime = DateTime.ParseExact(dateString, "yyyyMMddHH:mm:ss", CultureInfo.InvariantCulture);
                milli = Int16.Parse(new string(br.ReadChars(3)));
                datetime = datetime.AddMilliseconds(milli);
            }
            catch (FormatException)
            {
                IsValid = false;
                return; 
            }

            tagid = Int16.Parse(new string(br.ReadChars(5)));
            val = br.ReadDouble();
            status = br.ReadChar();
            marker = br.ReadChar();
            br.BaseStream.Seek(4, SeekOrigin.Current);
        }
    }

    public class DatReader
    {
        string[] floatfile_names;

        public DatReader(string path)
        {
            //string wildcard = Path.GetFileName(pattern);
            //string relPath = pattern.Substring(0, pattern.Length - wildcard.Length);
            //string fullPath = Path.GetFullPath(relPath);
            //floatfile_names = Directory.GetFiles(fullPath, wildcard);
            path = path.Replace("\"", "");
            floatfile_names = Directory.GetFiles(path, "* (Float).DAT");

            if (floatfile_names.Length < 1)
                throw new Exception("no input files");
        }

        public IEnumerable<string> GetFloatfiles()
        {
            for (int i = 0; i < floatfile_names.Length; i++)
            {
                yield return floatfile_names[i];
            }
        }

        public IEnumerable<DatTagRecord> ReadTagFile(string floatfile_name)
        {
            Dictionary<int, string> tagnames = new Dictionary<int, string>();
            string tagfile_name = floatfile_name.Replace(" (Float)", " (Tagname)");

            BinaryReader br = new BinaryReader(File.Open(tagfile_name, FileMode.Open, FileAccess.Read, FileShare.Read));
            byte ver = br.ReadByte();
            int yy = br.ReadByte() + 1900;
            byte mm = br.ReadByte();
            byte dd = br.ReadByte();
            int rowcount = br.ReadInt32();
            Console.WriteLine("{0} tags", rowcount);
            br.BaseStream.Seek(0xA1, SeekOrigin.Begin);

            for (int i = 0; i < rowcount; i++)
            {
                DatTagRecord rec = new DatTagRecord(br);
                yield return rec;
            }
            br.BaseStream.Close();
        }

        public IEnumerable<DatFloatRecord> ReadFloatFile(string filename)
        {
            BinaryReader br = new BinaryReader(File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.Read));
            byte ver = br.ReadByte();
            int yy = br.ReadByte() + 1900;
            byte mm = br.ReadByte();
            byte dd = br.ReadByte();
            int rowcount = br.ReadInt32();
            Console.WriteLine("{0} values", rowcount);
            br.BaseStream.Seek(0x121, SeekOrigin.Begin);

            DateTime last_progress_print = DateTime.Now;

            for (int i = 0; i < rowcount; i++)
            {
                DatFloatRecord rec = new DatFloatRecord(br);

                if ((DateTime.Now - last_progress_print).TotalSeconds > 3)
                {
                    Console.WriteLine($"{100 * i / rowcount}%");
                    last_progress_print = DateTime.Now;
                }

                yield return rec;
            }
            br.BaseStream.Close();
        }
    }
}
