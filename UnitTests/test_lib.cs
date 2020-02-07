using System;
using libDAT;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTests
{
    [TestClass]
    public class test_lib
    {
        [TestMethod]
        public void Test_DatReader()
        {
            int tag_count = 0;
            int val_count = 0;
            DatReader dr = new DatReader("../../InputData");
            foreach (string floatfile_name in dr.GetFloatfiles())
            {
                Console.Write($"{floatfile_name}\n");
                foreach (DatTagRecord tag in dr.ReadTagFile(floatfile_name))
                {
                    tag_count++;
                }
                foreach (DatFloatRecord val in dr.ReadFloatFile(floatfile_name))
                {
                    val_count++;
                }
            }
            Assert.AreEqual(tag_count, 32);
            Assert.AreEqual(val_count, 128);
        }
    }
}
