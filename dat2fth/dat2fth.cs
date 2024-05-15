using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;
using libDAT;
using libFTH;
using System.Runtime.InteropServices.ComTypes;

namespace dat2fth
{
    public static class Globals
    {
        public const int BATCH_SIZE = 1000;
    }

    class dat2fth
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Error: argument not found");
                Console.WriteLine("Usage: dat2fth ServerName PointPrefix [Path]");
                return;
            }
            PIAPI.Connect(args[0]);
            PIAPI.POINT_PREFIX = args[1];

            string path = ".";
            if (args.Length >= 3)
            {
                path = args[2];
            }

            // TODO: expose this for easy filtering
            List<string> allowed_tagnames = new List<string>();
            //allowed_tagnames.Add("AI\\FY59_1");

            DatReader dr = new DatReader(path);
            foreach (string floatfile_name in dr.GetFloatfiles())
            {
                Console.WriteLine("converting {0}", Path.GetFileName(floatfile_name));
                Dictionary<int, int> pointids = new Dictionary<int, int>();
                foreach (DatTagRecord tag in dr.ReadTagFile(floatfile_name))
                {
                    if (allowed_tagnames.Count > 0 && !allowed_tagnames.Contains(tag.name))
                        continue;
                    
                    string pointname = PIAPI.TagToPoint(tag.name);
                    pointids[tag.id] = PIAPI.GetPointNumber(pointname);
                }

                int batch_count = 0;
                Int32[] ptids = new Int32[Globals.BATCH_SIZE];
                double[] vs = new double[Globals.BATCH_SIZE];
                PITIMESTAMP[] ts = new PITIMESTAMP[Globals.BATCH_SIZE];

                // can use intermediate storage, more readable but less speed
                // List<FloatSnapshot> snaps = new List<FloatSnapshot>();

                foreach (DatFloatRecord val in dr.ReadFloatFile(floatfile_name))
                {
                    if (!val.IsValid) {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Error parsing date in file '{floatfile_name}'");
                        Console.ResetColor();
                        continue;
                    }
                        
                    if (!pointids.ContainsKey(val.tagid))
                        continue;
                    Int32 ptid = pointids[val.tagid];
                    ptids[batch_count] = ptid;
                    vs[batch_count] = val.val;
                    ts[batch_count] = new PITIMESTAMP(val.datetime);
                    batch_count++;

                    if (batch_count == Globals.BATCH_SIZE)
                    {
                        PIAPI.PutSnapshots(batch_count, ptids, vs, ts);
                        batch_count = 0;
                    }
                }
                if (batch_count > 0)
                {
                    PIAPI.PutSnapshots(batch_count, ptids, vs, ts);
                }
            }
        }
    }
}
