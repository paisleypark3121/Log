using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Log
{
    public interface ILog
    {
        bool Track(object message);
        Task<bool> TrackAsync(object message);
    }
}
