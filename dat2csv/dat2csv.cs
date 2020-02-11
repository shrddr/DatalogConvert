using libDAT;
using libFTH;
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
    public static class Globals
    {
        public const int BATCH_SIZE = 1000;
    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Error: argument not found");
                Console.WriteLine("Usage: dat2fth PointPrefix [path]");
                return;
            }
            PIAPI.POINT_PREFIX = args[0];

            string path = ".";
            if (args.Length >= 2)
            {
                path = args[1];
            }

            DatReader dr = new DatReader(path);
            uint converted = 0;

            List<string> tagnames = new List<string>();
            using (StreamWriter val_writer = new StreamWriter(File.Open("values.csv", FileMode.Create)))
            {
                val_writer.Write("@mode edit, t\n");
                val_writer.Write("@table pisnap\n");
                val_writer.Write("@istr tag, time, value\n");

                foreach (string infilename in dr.GetFloatfiles())
                {
                    Console.WriteLine("converting {0}", Path.GetFileName(infilename));
                    MakeFTH(dr, infilename, val_writer, tagnames);
                    converted++;
                }
            }
            Console.WriteLine("converted {0} files", converted);

            using (StreamWriter pts_writer = new StreamWriter(File.Open("del_points.csv", FileMode.Create)))
            {
                pts_writer.Write("@tabl pipoint,classic\n");
                pts_writer.Write("@mode delete\n");
                pts_writer.Write("@istr tag\n");
                foreach (var tagname in tagnames)
                {
                    pts_writer.Write($"{PIAPI.TagToPoint(tagname)}\n");
                }
            }
            using (StreamWriter pts_writer = new StreamWriter(File.Open("add_points.csv", FileMode.Create)))
            {
                pts_writer.Write("@tabl pipoint,classic\n");
                pts_writer.Write("@mode create\n");
                pts_writer.Write("@istr tag,pointsource,location1,location3,location4,span,zero,instrumenttag\n");
                foreach (var tagname in tagnames)
                {
                    pts_writer.Write($"{PIAPI.TagToPoint(tagname)},FTLD,1,1,1,100.,0.,{tagname}\n");
                }
            }
            Console.WriteLine("created {0} point definitions", tagnames.Count);
        }


        public static void MakeFTH(DatReader dr, string floatfile_name, StreamWriter outwriter, List<string> all_tagnames)
        {
            Dictionary<int, string> pointnames = new Dictionary<int, string>();

            foreach (DatTagRecord tag in dr.ReadTagFile(floatfile_name))
            {
                pointnames[tag.id] = PIAPI.TagToPoint(tag.name);
                if (!all_tagnames.Contains(tag.name))
                {
                    all_tagnames.Add(tag.name);
                }
            }

            int batch_count = 0;
            string batch = "";
            CultureInfo culture = new CultureInfo("en-US");

            foreach (DatFloatRecord val in dr.ReadFloatFile(floatfile_name))
            {
                if (val.status != 'U')
                {
                    batch += $"{pointnames[val.tagid]},{val.datetime.ToString("dd-MMM-yy HH:mm:ss.fff", culture)},{val.val}\n";
                    batch_count++;
                }
                if (batch_count == Globals.BATCH_SIZE)
                {
                    outwriter.Write(batch); // async?
                    batch_count = 0;
                    batch = "";
                }
            }
            if (batch_count > 0)
            {
                outwriter.Write(batch);
            }
        }
    }
}
