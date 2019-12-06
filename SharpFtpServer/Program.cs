using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using Proyecto_de_Distribuidos_01;
using ConsoleApp8;

namespace SharpFtpServer
{
    class Program
    {
        static void Main(string[] args)
        {
            using (FtpServer server = new FtpServer(IPAddress.IPv6Any, 21))
            {
                Chord.ChordStart();
                using (TCP_Server serverTCP = new TCP_Server(IPAddress.Any, Chord.port))
                {
                    serverTCP.Start();
                }
                server.Start();

                Console.WriteLine("Press any key to stop...");
                Console.ReadKey(true);
            }
        }
    }
}
