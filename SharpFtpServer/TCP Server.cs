using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Sockets;
using System.IO;
using System.Net;


namespace ConsoleApp8
{
    public class TCP_Server: IDisposable
    {
        TcpListener listener;
        IPEndPoint endpoint;
        private bool listening = false;
        private List<TCP_Client> activeConnections;
        bool disposed = false;

        public TCP_Server(IPAddress adress,int port)
        {
            endpoint = new IPEndPoint(adress, port);
        }
        public void Start()
        {
            listener = new TcpListener(endpoint);
            listening = true;
            listener.Start();

            activeConnections = new List<TCP_Client>();

            listener.BeginAcceptTcpClient(AcceptTcpClient, listener);
        }
        public void Stop()
        {

            listening = false;
            listener.Stop();

            listener = null;
        }
        public void AcceptTcpClient(IAsyncResult result)
        {
            if (listening)
            {
                listener.BeginAcceptTcpClient(AcceptTcpClient, listener);
                TcpClient client = listener.EndAcceptTcpClient(result);

                TCP_Client connection = new TCP_Client(client);

                activeConnections.Add(connection);

                ThreadPool.QueueUserWorkItem(connection.Answer, client);
            }
        }
        public void Dispose()
        {
            Dispose(true);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    Stop();

                    foreach (TCP_Client conn in activeConnections)
                    {
                        conn.Dispose();
                    }
                }
            }

            disposed = true;
        }
    }
}
