using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Log.Test
{
    public class LogEntry
    {
        public string LogName { get; set; }
        public string internal_id { get; set; }
        public string request { get; set; }
        public string internal_parameters { get; set; }
        public DateTime requestTime { get; set; }
        public string response { get; set; }
        public DateTime responseTime { get; set; }
    }
}
