using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;
using libDAT;
using libFTH;

namespace dat2fth
{
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
            
            /*Int32 ptid = PIAPI.GetPointNumber(args[1]);
            Console.WriteLine(ptid);
            PIAPI.PutSnapshot(ptid, 666);*/

            DatReader dr = new DatReader(path);
            foreach (string floatfile_name in dr.GetFloatfiles())
            {
                Console.WriteLine("converting {0}", Path.GetFileName(floatfile_name));
                Dictionary<int, int> pointids = new Dictionary<int, int>();
                foreach (DatTagRecord tag in dr.ReadTagFile(floatfile_name))
                {
                    string pointname = PIAPI.TagToPoint(tag.name);
                    pointids[tag.id] = PIAPI.GetPointNumber(pointname);
                }
                foreach (DatFloatRecord val in dr.ReadFloatFile(floatfile_name))
                {
                    Int32 ptid = pointids[val.tagid];
                    PIAPI.PutSnapshot(ptid, val.val, val.datetime);
                }
            }
        }
    }
}
