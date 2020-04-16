using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Log
{
    public class Utilities
    {
        public static string getInvertedTimeKey(DateTime dateTime)
        {
            if ((dateTime == null) || (dateTime == DateTime.MinValue))
                return null;

            return (DateTime.MaxValue.Ticks - dateTime.Ticks).ToString();
        }
    }
}
