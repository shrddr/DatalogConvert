using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace libFTH
{
    public struct PITIMESTAMP
    {
        public int month;
        public int year;
        public int day;
        public int hour;
        public int minute;
        public int tzinfo;
        public double second;

        public PITIMESTAMP(DateTime dt)
        {
            month = dt.Month;
            year = dt.Year;
            day = dt.Day;
            hour = dt.Hour;
            minute = dt.Minute;
            second = dt.Second + dt.Millisecond / 1000;
            tzinfo = 0;
        }
    }

    public class PIAPI
    {
        public static string POINT_PREFIX = "";
        public static string TagToPoint(string tagname)
        {
            return POINT_PREFIX + tagname.Replace('\\', '.');
        }

        private const string PIAPIDLL = "piapi.dll"; // 64bit!

        [DllImport(PIAPIDLL)]
        static extern Int32 piut_setservernode(string name);
        [DllImport(PIAPIDLL)]
        static extern Int32 pipt_findpoint(string name, out Int32 pointNumber);
        [DllImport(PIAPIDLL)]
        static extern Int32 pisn_putsnapshotx(Int32 ptnum, ref double drval, ref Int32 ival, byte[] bval, ref UInt32 bsize,
                                              ref Int32 istat, ref Int16 flags, ref PITIMESTAMP timestamp);
        [DllImport(PIAPIDLL)]
        static extern Int32 pisn_putsnapshotsx(Int32 count, Int32[] ptnum, double[] drval, Int32[] ival, byte[,] bval,
                                               UInt32[] bsize, Int32[] istat, Int16[] flags, PITIMESTAMP[] timestamp,
                                               out Int32[] errors);

        public static bool Connect(string serverName)
        {
            Int32 err = piut_setservernode(serverName);
            if (err != 0)
            {
                throw new Exception($"piut_setservernode: {err}");
            }
            return true;
        }

        public static Int32 GetPointNumber(string ptName)
        {
            Int32 pointNumber;
            int tagNameLength = ptName.Length;
            if (tagNameLength > 80)
            {
                throw new Exception("tagName > 80");
            }
            int err = pipt_findpoint(ptName, out pointNumber);
            if (err != 0)
            {
                throw new Exception($"pipt_findpoint: {err}");
            }
            return pointNumber;
        }

        public static bool PutSnapshot(int ptId, double v, DateTime dt)
        {
            Int32 ival = 0;
            UInt32 bsize = 0;
            Int32 istat = 0;
            Int16 flags = 0;
            PITIMESTAMP ts = new PITIMESTAMP(dt);

            Int32 err = pisn_putsnapshotx(ptId, ref v, ref ival, null, ref bsize, ref istat, ref flags, ref ts);
            if (err != 0)
            {
                throw new Exception($"pisn_putsnapshotx: {err}");
            }
            return true;
        }
    }
}
