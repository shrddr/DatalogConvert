using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace stats
{
    class Program
    {
        static int Offset(int record) { return 0x121 + 39 * record; }

        static void Main1(string[] args)
        {
            string fn = "2020 01 22 0000 (Tagname).DAT";
            BinaryReader br = new BinaryReader(File.Open(fn, FileMode.Open));
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
                char tagtype = br.ReadChar();
                int tagdtype = int.Parse(new string(br.ReadChars(2)));
                //Console.WriteLine("{0} -> {1} T:{2} DT:{3}", tagid, tagname, tagtype, tagdtype);
                tagnames[tagid] = tagname;
            }
            br.BaseStream.Close();

            fn = "2020 01 22 0000 (Float).DAT";
            br = new BinaryReader(File.Open(fn, FileMode.Open));
            ver = br.ReadByte();
            yy = br.ReadByte() + 1900;
            mm = br.ReadByte();
            dd = br.ReadByte();
            rowcount = br.ReadInt32();
            Console.WriteLine("{0} values", rowcount);

            br.BaseStream.Seek(0x121, SeekOrigin.Begin);

            Dictionary<int, int> stats = new Dictionary<int, int>();

            for (int i = 0; i < rowcount; i++)
            {
                br.BaseStream.Seek(1, SeekOrigin.Current);
                int y = int.Parse(new string(br.ReadChars(4)));
                int m = int.Parse(new string(br.ReadChars(2)));
                int d = int.Parse(new string(br.ReadChars(2)));
                char[] time = br.ReadChars(8);
                char[] milli = br.ReadChars(3);
                int tagid = int.Parse(new string(br.ReadChars(5)));
                double val = br.ReadDouble();
                char status = br.ReadChar();
                char marker = br.ReadChar();
                br.BaseStream.Seek(4, SeekOrigin.Current);

                if (stats.ContainsKey(tagid))
                {
                    stats[tagid]++;
                }
                else
                {
                    stats[tagid] = 1;
                }
            }
            br.BaseStream.Close();

            int count = 0;
            foreach (KeyValuePair<int, int> kvp in stats)
            {
                Console.WriteLine("{0} -> {1}", tagnames[kvp.Key], kvp.Value);
                count += kvp.Value;
            }
            Console.WriteLine(count);
            Console.ReadLine();
        }
    }
}
