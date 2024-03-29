﻿using ConsoleApp8;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Xml.Serialization;
using System.Threading;
using System.Collections.Concurrent;
using System.Text;
using Proyecto_de_Distribuidos_01;

//Este soy yo que estoy to tixa
namespace SharpFtpServer
{
    public static class YA
    {
        public static int ya = 0;
    }
    
    public class ClientConnection : IDisposable
    {
        private class DataConnectionOperation
        {
            public Func<NetworkStream, string, string> Operation { get; set; }
            public string Arguments { get; set; }
        }

        static ConcurrentDictionary<string,bool> dict = new ConcurrentDictionary<string, bool>();

        #region Copy Stream Implementations

        public static long CopyStream(Stream input, Stream output, int bufferSize)
        {
            byte[] buffer = new byte[bufferSize];
            int count = 0;
            long total = 0;

            while ((count = input.Read(buffer, 0, buffer.Length)) > 0)
            {
                output.Write(buffer, 0, count);
                total += count;
            }

            return total;
        }

        private static long CopyStreamAscii(Stream input, Stream output, int bufferSize)
        {
            char[] buffer = new char[bufferSize];
            int count = 0;
            long total = 0;

            using (StreamReader rdr = new StreamReader(input, Encoding.ASCII))
            {
                StreamWriter wtr = new StreamWriter(output, Encoding.ASCII);
                    while ((count = rdr.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        wtr.Write(buffer, 0, count);
                        total += count;
                    }
            }

            return total;
        }
        public static long StoreStream(Stream input, string pathname, int bufferSize, long ticks, long offset)
        {
            byte[] buffer = new byte[bufferSize];
            int count = 0;
            long total = 0;

            long i = offset;
            int j = bufferSize;
            while ((count = input.Read(buffer, bufferSize-j, j)) > -1)
            {
                j -= count;
                if (j == 0 || count==0)
                {
                    string partname = pathname + ticks + ".part" + i;
                    if (ticks == 0)
                        partname = pathname + ".part" + i;
                    List<IPEndPoint> endpoints = Whois(partname);
                    foreach (var end in endpoints)
                    {
                        TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                        if (!client.StoreFile(buffer, partname, bufferSize-j))
                            return 0;
                        client.Dispose();
                    }
                    total += bufferSize-j;
                    j = bufferSize;
                    i++;
                }
                if (count == 0)
                    break;
            }
            return total;

        }
        public static long RetrieveStreamm(string pathname, Stream output, long parts)
        {
            int total = 0;
            for (int i = 0; i < parts; i++)
            {
                string partname = pathname + ".part" + i;
                List<IPEndPoint> endpoints = Whois(partname);
                for (int j = 0; j < endpoints.Count; j++)
                {
                    var end = endpoints[j];
                    TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                    byte[] buffer = client.SearchFile(partname);
                    if (buffer.Length != 0)
                    {
                        client.Dispose();
                        output.Write(buffer, 0, buffer.Length);
                        total += buffer.Length;
                        break;
                    }
                    if (j == endpoints.Count - 1)
                        return 0;
                    client.Dispose();
                }
            }
            return total;

        }
        public static long RetrieveStreamAscii(string pathname, Stream output, long parts)
        {
            int total = 0;
            for (int i = 0; i < parts; i++)
            {
                string partname = pathname + ".part" + i;
                List<IPEndPoint> endpoints = Whois(partname);
                for (int j = 0; j < endpoints.Count; j++)
                {
                    var end = endpoints[j];
                    TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                    byte[] cbuffer = client.SearchFile(partname);
                    char[] buffer = Encoding.ASCII.GetChars(cbuffer);
                    if (buffer.Length != 0)
                    {
                        client.Dispose();
                        StreamWriter wtr = new StreamWriter(output, Encoding.ASCII) ;
                            wtr.Write(buffer, 0, buffer.Length);
                            total += buffer.Length;
                        break;
                    }
                    if (j == endpoints.Count - 1)
                        return 0;
                    client.Dispose();
                }
            }
            return total;

        }
        private static long StoreStreamAscii(Stream input, string pathname, int bufferSize, long ticks, long offset)
        {
            char[] buffer = new char[bufferSize];
            int count = 0;
            long total = 0;

            using (StreamReader rdr = new StreamReader(input, Encoding.ASCII))
            {
                long i = offset;
                int j = bufferSize;
                while ((count = rdr.Read(buffer, bufferSize - j, j)) > -1)
                {
                    j -= count;
                    if (j == 0 || count == 0)
                    {
                        string partname = pathname + ticks + ".part" + i;
                        if (ticks == 0)
                            partname = pathname + ".part" + i;
                        List<IPEndPoint> endpoints = Whois(partname);
                        foreach (var end in endpoints)
                        {
                            TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                            if (!client.StoreFile(buffer, partname, bufferSize-j))
                                return 0;
                            client.Dispose();
                        }
                        total += bufferSize - j;
                        j = bufferSize;
                        i++;
                    }
                    if (count == 0)
                        break;
                }
            }
            return total;
        }

        private static List<IPEndPoint> Whois(string partname)
        {
            return Chord.WHOISTHIS(partname);
        }

        private long CopyStream(Stream input, Stream output)
        {
            Stream limitedStream = output; // new RateLimitingStream(output, 131072, 0.5);

            if (_connectionType == TransferType.Image)
            {
                return CopyStream(input, limitedStream, 4096);
            }
            else
            {
                return CopyStreamAscii(input, limitedStream, 4096);
            }
        }

        private long StoreStream(Stream input, string output, long ticks, long offset)
        {
            string limitedStream = output;

            if (_connectionType == TransferType.Image)
            {
                return StoreStream(input, limitedStream, 1 << 18, ticks, offset);
            }
            else
            {
                return StoreStreamAscii(input, limitedStream, 1 << 18, ticks, offset);
            }
        }
        private long RetrieveStream(string pathname, Stream output, long parts)
        {
            Stream limitedStream = output;

            if (_connectionType == TransferType.Image)
            {
                return RetrieveStreamm(pathname, limitedStream, parts);
            }
            else
            {
                return RetrieveStreamAscii(pathname, limitedStream, parts);
            }
        }

        #endregion

        #region Enums

        private enum TransferType
        {
            Ascii,
            Ebcdic,
            Image,
            Local,
        }

        private enum FormatControlType
        {
            NonPrint,
            Telnet,
            CarriageControl,
        }

        private enum DataConnectionType
        {
            Passive,
            Active,
        }

        private enum FileStructureType
        {
            File,
            Record,
            Page,
        }

        #endregion

        private bool _disposed = false;

        private TcpListener _passiveListener;

        private TcpClient _controlClient;
        private TcpClient _dataClient;

        private NetworkStream _controlStream;
        private StreamReader _controlReader;
        private StreamWriter _controlWriter;

        private TransferType _connectionType = TransferType.Ascii;
        private FormatControlType _formatControlType = FormatControlType.NonPrint;
        private DataConnectionType _dataConnectionType = DataConnectionType.Active;
        private FileStructureType _fileStructureType = FileStructureType.File;

        private string _username;
        public static string _root= @"C:\Test";
        private string _currentDirectory;
        private IPEndPoint _dataEndpoint;
        private IPEndPoint _remoteEndPoint;

        private X509Certificate _cert = null;
        private SslStream _sslStream;

        private string _clientIP;

        private User _currentUser;

        private List<string> _validCommands;

        public ClientConnection(TcpClient client)
        {
            _controlClient = client;

            _validCommands = new List<string>();
        }

        private string CheckUser()
        {
            if (_currentUser == null)
            {
                return "230 User logged in";
            }

            return null;
        }

        public void HandleClient(object obj)
        {
            _remoteEndPoint = (IPEndPoint)_controlClient.Client.RemoteEndPoint;

            _clientIP = _remoteEndPoint.Address.ToString();

            _controlStream = _controlClient.GetStream();

            _controlReader = new StreamReader(_controlStream);
            _controlWriter = new StreamWriter(_controlStream);

            _controlWriter.WriteLine("220 Service Ready.");
            _controlWriter.Flush();

            _validCommands.AddRange(new string[] { "AUTH", "USER", "PASS", "QUIT", "HELP", "NOOP" });

            string line;

            _dataClient = new TcpClient();

            string renameFrom = null;

            try
            {
                while ((line = _controlReader.ReadLine()) != null)
                {
                    string response = null;

                    string[] command = line.Split(' ');

                    string cmd = command[0].ToUpperInvariant();
                    string arguments = command.Length > 1 ? line.Substring(command[0].Length + 1) : null;
                    Console.WriteLine(cmd);
                    if (arguments != null && arguments.Trim().Length == 0)
                    {
                        arguments = null;
                    }

                    LogEntry logEntry = new LogEntry
                    {
                        Date = DateTime.Now,
                        CIP = _clientIP,
                        CSUriStem = arguments
                    };

                    if (!_validCommands.Contains(cmd))
                    {
                        response = CheckUser();
                    }

                    if (cmd != "RNTO")
                    {
                        renameFrom = null;
                    }

                    if (response == null)
                    {
                        switch (cmd)
                        {
                            case "USER":
                                response = User(arguments);
                                break;
                            case "PASS":
                                response = Password(arguments);
                                logEntry.CSUriStem = "******";
                                break;
                            case "CWD":
                                response = ChangeWorkingDirectory(arguments);
                                break;
                            case "CDUP":
                                response = ChangeWorkingDirectory("..");
                                break;
                            case "QUIT":
                                response = "221 Service closing control connection";
                                break;
                            case "REIN":
                                _currentUser = null;
                                _username = null;
                                _passiveListener = null;
                                _dataClient = null;

                                response = "220 Service ready for new user";
                                break;
                            case "PORT":
                                response = Port(arguments);
                                logEntry.CPort = _dataEndpoint.Port.ToString();
                                break;
                            case "PASV":
                                response = Passive();
                                logEntry.SPort = ((IPEndPoint)_passiveListener.LocalEndpoint).Port.ToString();
                                break;
                            case "TYPE":
                                response = Type(command[1], command.Length == 3 ? command[2] : null);
                                logEntry.CSUriStem = command[1];
                                break;
                            case "STRU":
                                response = Structure(arguments);
                                break;
                            case "MODE":
                                response = Mode(arguments);
                                break;
                            case "RNFR":
                                renameFrom = arguments;
                                response = "350 Requested file action pending further information";
                                break;
                            case "RNTO":
                                response = Rename(renameFrom, arguments);
                                break;
                            case "DELE":
                                response = Delete(arguments);
                                break;
                            case "RMD":
                                response = RemoveDir(arguments);
                                break;
                            case "MKD":
                                response = CreateDir(arguments);
                                break;
                            case "PWD":
                                response = PrintWorkingDirectory();
                                break;
                            case "RETR":
                                response = Retrieve(arguments);
                                logEntry.Date = DateTime.Now;
                                break;
                            case "STOR":
                                response = Store(arguments);
                                logEntry.Date = DateTime.Now;
                                break;
                            case "STOU":
                                response = StoreUnique();
                                logEntry.Date = DateTime.Now;
                                break;
                            case "APPE":
                                response = Append(arguments);
                                logEntry.Date = DateTime.Now;
                                break;
                            case "LIST":
                                response = List(arguments ?? _currentDirectory);
                                logEntry.Date = DateTime.Now;
                                break;
                            case "SYST":
                                response = "215 UNIX Type: L8";
                                break;
                            case "NOOP":
                                response = "200 OK";
                                break;
                            case "ACCT":
                                response = "200 OK";
                                break;
                            case "ALLO":
                                response = "200 OK";
                                break;
                            case "NLST":
                                response = "502 Command not implemented";
                                break;
                            case "SITE":
                                response = "502 Command not implemented";
                                break;
                            case "STAT":
                                response = "502 Command not implemented";
                                break;
                            case "HELP":
                                response = "502 Command not implemented";
                                break;
                            case "SMNT":
                                response = "502 Command not implemented";
                                break;
                            case "REST":
                                response = "502 Command not implemented";
                                break;
                            case "ABOR":
                                response = "502 Command not implemented";
                                break;

                            // Extensions defined by rfc 2228
                            case "AUTH":
                                response = "502 Command not allowed";
                                //response = Auth(arguments);
                                break;

                            // Extensions defined by rfc 2389
                            case "FEAT":
                                response = FeatureList();
                                break;
                            case "OPTS":
                                response = Options(arguments);
                                break;

                            // Extensions defined by rfc 3659
                            case "MDTM":
                                response = FileModificationTime(arguments);
                                break;
                            case "SIZE":
                                response = FileSize(arguments);
                                break;

                            // Extensions defined by rfc 2428
                            case "EPRT":
                                response = EPort(arguments);
                                logEntry.CPort = _dataEndpoint.Port.ToString();
                                break;
                            case "EPSV":
                                response = EPassive();
                                logEntry.SPort = ((IPEndPoint)_passiveListener.LocalEndpoint).Port.ToString();
                                break;

                            default:
                                response = "502 Command not implemented";
                                break;
                        }
                    }

                    logEntry.CSMethod = cmd;
                    logEntry.CSUsername = _username;
                    logEntry.SCStatus = response.Substring(0, response.IndexOf(' '));



                    if (_controlClient == null || !_controlClient.Connected)
                    {
                        break;
                    }
                    else
                    {
                        _controlWriter.WriteLine(response);
                        _controlWriter.Flush();

                        if (response.StartsWith("221"))
                        {
                            break;
                        }

                        if (cmd == "AUTH")
                        {
                            //_cert = new X509Certificate("server.cer");

                            //_sslStream = new SslStream(_controlStream);

                            //_sslStream.AuthenticateAsServer(_cert);

                            //_controlReader = new StreamReader(_sslStream);
                            //_controlWriter = new StreamWriter(_sslStream);
                        }
                    }
                }
            }
            catch (Exception ex)
            {

            }

            Dispose();
        }

        private bool IsPathValid(string path)
        {
            return path.StartsWith(_root);
        }

        private string NormalizeFilename(string path)
        {
            if (path == null)
            {
                path = string.Empty;
            }

            if (path == "/")
            {
                return _root;
            }
            else if (path.StartsWith("/"))
            {
                path = new FileInfo(Path.Combine(_root, path.Substring(1))).FullName;
            }
            else
            {
                path = new FileInfo(Path.Combine(_currentDirectory, path)).FullName;
            }

            return IsPathValid(path) ? path : null;
        }

        #region FTP Commands

        private string FeatureList()
        {
            _controlWriter.WriteLine("211- Extensions supported:");
            _controlWriter.WriteLine(" MDTM");
            _controlWriter.WriteLine(" SIZE");
            return "211 End";
        }

        private string Options(string arguments)
        {
            return "200 Looks good to me...";
        }

        private string Auth(string authMode)
        {
            if (authMode == "TLS")
            {
                return "234 Enabling TLS Connection";
            }
            else
            {
                return "504 Unrecognized AUTH mode";
            }
        }

        private string User(string username)
        {
            _username = username;

            return "331 Username ok, need password";
        }

        private string Password(string password)
        {
            _currentUser = UserStore.Validate(_username, password);

            if (_currentUser != null)
            {
                _root = _currentUser.HomeDir;
                _currentDirectory = _root;
                if (YA.ya==0)
                {
                    Monitor.Enter(_root);
                    YA.ya++;
                    Monitor.Exit(_root);
                    Directory.CreateDirectory(_root);
                    foreach (var a in Directory.GetDirectories(_root))
                        DeleteDirectory(a);
                    foreach (var b in Directory.GetFiles(_root))
                        File.Delete(b);
                    string pathname = NormalizeFilename(_root);
                    byte[] buffer;
                    using (var fs = DirectoryMethods.CreateDirectory(pathname))
                    {
                        buffer = new byte[fs.Length];
                        fs.Read(buffer, 0, buffer.Length);
                    }
                    var dirend = Whois(GetDirectoryName(pathname));
                    foreach (var end in dirend)
                    {
                        TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                        if (!client.StoreFile(buffer, GetDirectoryName(pathname)))
                            return "530 Not logged in";
                    }
                }
                return "230 User logged in";
            }
            else
            {
                return "530 Not logged in";
            }
        }

        private void DeleteDirectory(string root)
        {
            foreach(var a in Directory.GetDirectories(root))
                DeleteDirectory(a);
            foreach (var b in Directory.GetFiles(root))
                File.Delete(b);
            Directory.Delete(root);
        }

        private string ChangeWorkingDirectory(string pathname)
        {
            if (pathname == "/")
            {
                _currentDirectory = _root;
            }
            else
            {

                if (pathname.StartsWith("/"))
                {
                    pathname = pathname.Substring(1).Replace('/', '\\');
                    _currentDirectory = Path.Combine(_root, pathname);
                }
                else
                {
                    pathname = pathname.Replace('/', '\\');
                    _currentDirectory = Path.Combine(_currentDirectory, pathname);
                }


                    if (!IsPathValid(_currentDirectory))
                    {
                        _currentDirectory = _root;
                    }
                
            }
            if(!GetDirectory(_currentDirectory))
            CreateDirectory(_currentDirectory);
            return "250 Changed to new directory";
        }

        private string Port(string hostPort)
        {
            _dataConnectionType = DataConnectionType.Active;

            string[] ipAndPort = hostPort.Split(',');

            byte[] ipAddress = new byte[4];
            byte[] port = new byte[2];

            for (int i = 0; i < 4; i++)
            {
                ipAddress[i] = Convert.ToByte(ipAndPort[i]);
            }

            for (int i = 4; i < 6; i++)
            {
                port[i - 4] = Convert.ToByte(ipAndPort[i]);
            }

            if (BitConverter.IsLittleEndian)
                Array.Reverse(port);

            _dataEndpoint = new IPEndPoint(new IPAddress(ipAddress), BitConverter.ToInt16(port, 0));

            return "200 Data Connection Established";
        }

        private string EPort(string hostPort)
        {
            _dataConnectionType = DataConnectionType.Active;

            char delimiter = hostPort[0];

            string[] rawSplit = hostPort.Split(new char[] { delimiter }, StringSplitOptions.RemoveEmptyEntries);

            char ipType = rawSplit[0][0];

            string ipAddress = rawSplit[1];
            string port = rawSplit[2];

            _dataEndpoint = new IPEndPoint(IPAddress.Parse(ipAddress), int.Parse(port));

            return "200 Data Connection Established";
        }

        private string Passive()
        {
            _dataConnectionType = DataConnectionType.Passive;

            IPAddress localIp = ((IPEndPoint)_controlClient.Client.LocalEndPoint).Address;

            _passiveListener = new TcpListener(localIp, 0);
            _passiveListener.Start();

            IPEndPoint passiveListenerEndpoint = (IPEndPoint)_passiveListener.LocalEndpoint;

            byte[] address = passiveListenerEndpoint.Address.GetAddressBytes();
            short port = (short)passiveListenerEndpoint.Port;

            byte[] portArray = BitConverter.GetBytes(port);

            if (BitConverter.IsLittleEndian)
                Array.Reverse(portArray);

            return string.Format("227 Entering Passive Mode ({0},{1},{2},{3},{4},{5})", address[0], address[1], address[2], address[3], portArray[0], portArray[1]);
        }

        private string EPassive()
        {
            _dataConnectionType = DataConnectionType.Passive;

            IPAddress localIp = ((IPEndPoint)_controlClient.Client.LocalEndPoint).Address;

            _passiveListener = new TcpListener(localIp, 0);
            _passiveListener.Start();

            IPEndPoint passiveListenerEndpoint = (IPEndPoint)_passiveListener.LocalEndpoint;

            return string.Format("229 Entering Extended Passive Mode (|||{0}|)", passiveListenerEndpoint.Port);
        }

        private string Type(string typeCode, string formatControl)
        {
            switch (typeCode.ToUpperInvariant())
            {
                case "A":
                    _connectionType = TransferType.Ascii;
                    break;
                case "I":
                    _connectionType = TransferType.Image;
                    break;
                default:
                    return "504 Command not implemented for that parameter";
            }

            if (!string.IsNullOrWhiteSpace(formatControl))
            {
                switch (formatControl.ToUpperInvariant())
                {
                    case "N":
                        _formatControlType = FormatControlType.NonPrint;
                        break;
                    default:
                        return "504 Command not implemented for that parameter";
                }
            }

            return string.Format("200 Type set to {0}", _connectionType);
        }

        private string Delete(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
               
                if (GetDirectory(pathname,false))
                {
                    if (LockDirectory(pathname, false))
                    {
                        List<MyDirectory> directories = new List<MyDirectory>();
                        string directoryname = GetDirectoryName(pathname);
                        XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
                        directories = serializer2.Deserialize(new StreamReader(directoryname)) as List<MyDirectory>;
                        directories.RemoveAll(i => i.Name == pathname);
                        Tuple<byte[], FileStream> tuple = null;
                        FileStream fs;
                        if (TCP_Client.PathaIDFS.TryGetValue(pathname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                            fs = tuple.Item2;
                        else
                            fs = new FileStream(GetDirectoryName(pathname), FileMode.Open, FileAccess.Write, FileShare.None);
                        fs.Position = 0;
                        StreamWriter w = new StreamWriter(fs);//quite using
                        w.BaseStream.SetLength(0);
                        w.BaseStream.Flush();
                                serializer2.Serialize(w, directories);
                                var dirend = Whois(GetDirectoryName(pathname));
                                for(int i=0;i<dirend.Count;i++)
                                {
                                    var end = dirend[i];
                                    TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                                    if (!client.StoreFile(fs, GetDirectoryName(pathname)))
                                        return "552 Requested file action aborted.";
                                }
                }
                    else
                    {
                        return "550 File Not Found";
                    }
                }
            else
            {
                return "550 File Not Found";
            }
                UnlockDirectory(pathname);
                return "250 Requested file action okay, completed";
        }

            return "550 File Not Found";
        }
        private bool RemoveDirectory(string pathname)
        {
            if (GetDirectory(pathname))
            {
                if (!LockDirectory(pathname, false))////////////////////////////// Esto está raro. Revisar por si acaso
                {
                    return false;
                }
                Tuple<byte[], FileStream> tuple = null;
                FileStream fs;
                if (TCP_Client.PathaIDFS.TryGetValue(GetDirectoryName(pathname), out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                {
                    fs = tuple.Item2;
                }
                else
                    fs = new FileStream(GetDirectoryName(pathname), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                fs.Position = 0;
                    List<MyDirectory> directories = new List<MyDirectory>();
                    string directoryname = GetDirectoryName(pathname);
                    XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
                StreamReader sr = new StreamReader(fs);
                    directories = serializer2.Deserialize(sr) as List<MyDirectory>;
                    foreach(var dir in directories.Where(i=>!i.IsFile))
                    {
                        if (RemoveDirectory(dir.Name))
                            continue;
                        UnlockDirectory(pathname);
                        return false;
                    }
                
                var dirend = Whois(GetDirectoryName(pathname));
                foreach (var end in dirend)
                {
                    TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                    if (!client.DeleteFile(GetDirectoryName(pathname)))
                    {
                        UnlockDirectory(pathname);
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        private string RemoveDir(string pathname)
        {
            
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                if (GetDirectory(ParentDirectory(pathname)))
                {
                    if (!LockDirectory(ParentDirectory(pathname), false))
                    {

                        return "450 Requested file action not taken";
                    }
                    Tuple<byte[], FileStream> tuple = null;
                    FileStream fs = null;
                    if (TCP_Client.PathaIDFS.TryGetValue(ParentDirectory(pathname), out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                        fs = tuple.Item2;
                    else
                        fs = new FileStream(ParentDirectory(pathname), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                    fs.Position = 0;
                    List<MyDirectory> directories = new List<MyDirectory>();
                    string directoryname = ParentDirectory(pathname);
                    XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
                    StreamReader sr = new StreamReader(fs);
                    directories = serializer2.Deserialize(sr) as List<MyDirectory>;
                    directories.RemoveAll(i => i.Name == pathname);
                    StreamWriter w = new StreamWriter(fs);
                    w.BaseStream.SetLength(0);
                    w.BaseStream.Flush();
                    serializer2.Serialize(w, directories);
                    var dirend = Whois((ParentDirectory(pathname)));
                    foreach (var end in dirend)
                    {
                        TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                        if (!client.StoreFile(fs, ParentDirectory(pathname)))
                        {
                            UnlockDirectory(ParentDirectory(pathname));
                            return "450 Requested file action not taken";
                        }
                    }
                    UnlockDirectory(ParentDirectory(pathname));
                }
                if (RemoveDirectory(pathname))
                {
                }
                else
                {
                    return "550 Directory Not Found";
                }

                return "250 Requested file action okay, completed";
            }

            return "550 Directory Not Found";
        }

        private string CreateDir(string pathname)
        {
            pathname = NormalizeFilename(pathname);
            if (pathname != null)
            {
                return CreateDirectory(pathname);
            }

            return "550 Directory Not Found";
        }

        private string CreateDirectory(string pathname)
        {
            string[] a = pathname.Split('\\');
            if (a[a.Length - 1] == "infod")
            {
                pathname = a[0];
                for (int i = 1; i < a.Length - 1; i++)
                    pathname += @"\" + a[i];
            }
            if (!GetDirectory(ParentDirectory(pathname)))
                CreateDirectory(ParentDirectory(pathname));
                if (!GetDirectory(pathname))
                {
                    string c = ParentDirectory(pathname);
                    byte[] buffer;
                    using (var fs = DirectoryMethods.CreateDirectory(pathname))
                    {
                        buffer = new byte[fs.Length];
                        fs.Read(buffer, 0, buffer.Length);

                    }

                    var dirend = Whois(GetDirectoryName(pathname));
                    foreach (var end in dirend)
                    {
                        TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                        if (!client.StoreFile(buffer, GetDirectoryName(pathname)))
                            return "550 Directory Not Copied";
                    }

                    if (GetDirectory(ParentDirectory(pathname)))
                    {
                        using (FileStream f = new FileStream(ParentDirectory(pathname), FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                        {
                            DirectoryMethods.AddAtDirectory(f, pathname, pathname, 0);
                            f.Position = 0;
                            buffer = new byte[f.Length];
                            f.Read(buffer, 0, buffer.Length);
                            f.Dispose();

                        }
                        var dirend1 = Whois(ParentDirectory(pathname));
                        foreach (var end in dirend1)
                        {
                            TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                            if (!client.StoreFile(buffer, ParentDirectory(pathname)))
                                return "550 Parent Directory Not Copied";
                        }
                    }
                    else
                    {
                        return "550 Directory not found";
                    }
                }
                else
                {
                    return "550 Directory already exists";
                }
                return "250 Requested file action okay, completed";
        }

        private string ParentDirectory(string pathname)
        {
            string[] parts = pathname.Split('\\');
            int a = 1;
            if (parts[parts.Length - 1] == "infod")
                a = 2;
            string name = parts[0];
            for (int i = 1; i < parts.Length - a; i++)
                name += @"\" + parts[i];
            name += @"\infod";
            return name;
        }

        private string FileModificationTime(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                if (GetDirectory(pathname))
                {
                    List<MyDirectory> directories = new List<MyDirectory>();
                    string directoryname = GetDirectoryName(pathname);
                    XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
                    directories = serializer2.Deserialize(new StreamReader(directoryname)) as List<MyDirectory>;
                    foreach (var file in directories.Where(i => i.Name==pathname))
                        return string.Format("213 {0}", file.Date.ToString("yyyyMMddHHmmss.fff"));
                }
            }

            return "550 File Not Found";
        }

        private string FileSize(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                if (GetDirectory(pathname))
                {
                    List<MyDirectory> directories = new List<MyDirectory>();
                    string directoryname = GetDirectoryName(pathname);
                    XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
                    directories = serializer2.Deserialize(new StreamReader(directoryname)) as List<MyDirectory>;
                    foreach (var file in directories.Where(i => i.Name == pathname))
                        return string.Format("213 {0}", file.Size);
                }
            }

            return "550 File Not Found";
        }

        private string Retrieve(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                    var state = new DataConnectionOperation { Arguments = pathname, Operation = RetrieveOperation };

                    SetupDataConnectionOperation(state);

                    return string.Format("150 Opening {0} mode data transfer for RETR", _dataConnectionType);
            }

            return "550 File Not Found";
        }

        private string Store(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                var state = new DataConnectionOperation { Arguments = pathname, Operation = StoreOperation };

                SetupDataConnectionOperation(state);

                return string.Format("150 Opening {0} mode data transfer for STOR", _dataConnectionType);
            }

            return "450 Requested file action not taken";
        }

        private string Append(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                var state = new DataConnectionOperation { Arguments = pathname, Operation = AppendOperation };

                SetupDataConnectionOperation(state);

                return string.Format("150 Opening {0} mode data transfer for APPE", _dataConnectionType);
            }

            return "450 Requested file action not taken";
        }

        private string StoreUnique()
        {
            string pathname = NormalizeFilename(new Guid().ToString());

            var state = new DataConnectionOperation { Arguments = pathname, Operation = StoreOperation };

            SetupDataConnectionOperation(state);

            return string.Format("150 Opening {0} mode data transfer for STOU", _dataConnectionType);
        }

        private string PrintWorkingDirectory()
        {
            string current = _currentDirectory.Replace(_root, string.Empty).Replace('\\', '/');

            if (current.Length == 0)
            {
                current = "/";
            }

            return string.Format("257 \"{0}\" is current directory.", current); ;
        }

        private string List(string pathname)
        {
            pathname = NormalizeFilename(pathname);

            if (pathname != null)
            {
                var state = new DataConnectionOperation { Arguments = pathname, Operation = ListOperation };

                SetupDataConnectionOperation(state);

                return string.Format("150 Opening {0} mode data transfer for LIST", _dataConnectionType);
            }

            return "450 Requested file action not taken";
        }

        private string Structure(string structure)
        {
            switch (structure)
            {
                case "F":
                    _fileStructureType = FileStructureType.File;
                    break;
                case "R":
                case "P":
                    return string.Format("504 STRU not implemented for \"{0}\"", structure);
                default:
                    return string.Format("501 Parameter {0} not recognized", structure);
            }

            return "200 Command OK";
        }

        private string Mode(string mode)
        {
            if (mode.ToUpperInvariant() == "S")
            {
                return "200 OK";
            }
            else
            {
                return "504 Command not implemented for that parameter";
            }
        }

        private string Rename(string renameFrom, string renameTo)
        {
            if (string.IsNullOrWhiteSpace(renameFrom) || string.IsNullOrWhiteSpace(renameTo))
            {
                return "450 Requested file action not taken";
            }
            
            renameFrom = NormalizeFilename(renameFrom);
            renameTo = NormalizeFilename(renameTo);
            if(!GetDirectory(renameFrom))
                return "450 Requested file action not taken";
            if (!LockDirectory(renameFrom,false))
            {

                return "450 Requested file action not taken";
            }
            Tuple<byte[], FileStream> tuple = null;
            FileStream fs;
            string pathname = GetDirectoryName(renameFrom);
            if (TCP_Client.PathaIDFS.TryGetValue(pathname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                fs = tuple.Item2;
            else
                fs = new FileStream(pathname, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            
                var dirend1 = Whois(GetDirectoryName(renameTo));
                foreach (var end in dirend1)
                {
                    TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                    if (!client.StoreFile(fs, GetDirectoryName(renameTo)))
                    {
                        UnlockDirectory(GetDirectoryName(renameTo));
                        return "450 Requested file action not taken";
                    }
                }
            
            UnlockDirectory(renameFrom);
            if (GetDirectory(ParentDirectory(renameFrom)))
            {
                if (!LockDirectory(ParentDirectory(renameFrom), false))
                {

                    return "450 Requested file action not taken";
                }
                tuple = null;
                if (TCP_Client.PathaIDFS.TryGetValue(ParentDirectory(renameFrom), out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                    fs = tuple.Item2;
                else
                    fs = new FileStream(ParentDirectory(renameFrom), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                fs.Position = 0;
                List<MyDirectory> directories = new List<MyDirectory>();
                string directoryname = ParentDirectory(renameFrom);
                XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
                StreamReader sr = new StreamReader(fs);
                directories = serializer2.Deserialize(sr) as List<MyDirectory>;
                bool wasdone = false;
                foreach (var dir in directories.Where(i => i.Name == renameFrom))
                {
                    dir.Name = renameTo;
                    wasdone = true;
                }
                if (!wasdone)
                    return "450 Requested file action not taken";
                StreamWriter w = new StreamWriter(fs);
                w.BaseStream.SetLength(0);
                w.BaseStream.Flush();
                serializer2.Serialize(w, directories);
                    var dirend = Whois((ParentDirectory(renameFrom)));
                    foreach (var end in dirend)
                    {
                        TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                        if (!client.StoreFile(fs, ParentDirectory(renameFrom)))
                        {
                            UnlockDirectory(ParentDirectory(renameFrom));
                            return "450 Requested file action not taken";
                        }
                    }
                    UnlockDirectory(ParentDirectory(renameFrom));
                }
                else
                {
                    return "450 Requested file action not taken";
                }

                return "250 Requested file action okay, completed";
            }

        #endregion

        #region DataConnection Operations

        private void HandleAsyncResult(IAsyncResult result)
        {
            if (_dataConnectionType == DataConnectionType.Active)
            {
                _dataClient.EndConnect(result);
            }
            else
            {
                _dataClient = _passiveListener.EndAcceptTcpClient(result);
            }
        }

        private void SetupDataConnectionOperation(DataConnectionOperation state)
        {
            if (_dataConnectionType == DataConnectionType.Active)
            {
                _dataClient = new TcpClient(_dataEndpoint.AddressFamily);
                _dataClient.BeginConnect(_dataEndpoint.Address, _dataEndpoint.Port, DoDataConnectionOperation, state);
            }
            else
            {
                _passiveListener.BeginAcceptTcpClient(DoDataConnectionOperation, state);
            }
        }

        private void DoDataConnectionOperation(IAsyncResult result)
        {
            HandleAsyncResult(result);

            DataConnectionOperation op = result.AsyncState as DataConnectionOperation;

            string response;

            using (NetworkStream dataStream = _dataClient.GetStream())
            {
                response = op.Operation(dataStream, op.Arguments);
            }

            _dataClient.Close();
            _dataClient = null;

            _controlWriter.WriteLine(response);
            _controlWriter.Flush();
        }

        private string RetrieveOperation(NetworkStream dataStream, string pathname)
        {
            long bytes = 0;
            if (!GetDirectory(pathname,false))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            string descname = "";
            List<MyDirectory> directories = new List<MyDirectory>();
            string directoryname = ParentDirectory(pathname);
            XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            StreamReader sr = new StreamReader(directoryname);
            directories = serializer2.Deserialize(sr) as List<MyDirectory>;
            foreach (var b in directories.Where(i => i.Name == pathname))
                descname = b.Realname;
            if (!GetDescriptor(descname))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            if (!LockDescriptor(descname,false))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            List<MyFileDescriptor> files = new List<MyFileDescriptor>();
            XmlSerializer serializer = new XmlSerializer(files.GetType(), new XmlRootAttribute("MyFileDescriptor"));
            Tuple<byte[], FileStream> tuple = null;
            FileStream fs;
            bool LoCogiDeAllaArriba = false;
            if (TCP_Client.PathaIDFS.TryGetValue(descname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
            {
                fs = tuple.Item2;
                LoCogiDeAllaArriba = true;
            }
            else
                fs = new FileStream(descname, FileMode.Open, FileAccess.Read, FileShare.Read);
            fs.Position = 0;
            var elReader = new StreamReader(fs);
            files = serializer.Deserialize(elReader) as List<MyFileDescriptor>;
            if(!LoCogiDeAllaArriba)
            {
                fs.Dispose();
            }
            else
                UnlockDescriptor(descname);
            long size = files[0].Size;
            long parts = files[0].Parts;
            string filename = files[0].Name;
                bytes += RetrieveStream(filename,dataStream, parts);
                //if (bytes == 0)
                //    return "552 Requested file action aborted.";
           

            return "226 Closing data connection, file transfer successful";
        }

        private string StoreOperation(NetworkStream dataStream, string pathname)
        {
            long ticks = DateTime.Now.Ticks;
            if (!GetDirectory(pathname, false))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            List<MyDirectory> directories = new List<MyDirectory>();
            string directoryname = ParentDirectory(pathname);
            XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            StreamReader sr = new StreamReader(directoryname);
            directories = serializer2.Deserialize(sr) as List<MyDirectory>;
            sr.Dispose();
            string descname = pathname + ticks;
            bool yaesta = false;
            foreach (var b in directories.Where(i => i.Name == pathname))
            {
                yaesta = true;
            }
            long bytes = 0;

            bytes = StoreStream(dataStream, pathname, ticks, 0);
            //if (bytes == 0)
            //    return "552 Requested file action aborted.";
            string dirname = ParentDirectory(pathname);
            if (!GetDirectory(pathname, false))
                return "552 Requested file action aborted.";
            if (!LockDirectory(ParentDirectory(pathname), false))
                return "552 Requested file action aborted.";
            Tuple<byte[], FileStream> tuple = null;
            FileStream directory;
            if (TCP_Client.PathaIDFS.TryGetValue(dirname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                directory = tuple.Item2;
            else
                directory = new FileStream(dirname, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            directory.Position = 0;
            byte[] buffer;
            FileStream descriptor = DirectoryMethods.CreateDescriptor(descname, bytes);
            List<IPEndPoint> descEnd = Whois(descname);
            descriptor.Position = 0;
            buffer = new byte[descriptor.Length];
            descriptor.Read(buffer, 0, buffer.Length);
            descriptor.Position = 0;
            descriptor.Dispose();
            foreach (var end in descEnd)
            {
                TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                if (!client.StoreFile(buffer, descname))
                    return "552 Requested file action aborted.";
            }
            if (yaesta)
            {
                DirectoryMethods.ChangeAtDirectory(directory, pathname, descname, bytes, true);
            }
            else
            {
                DirectoryMethods.AddAtDirectory(directory, pathname, descname, bytes, true);
            }
            List<IPEndPoint> dirEnd = Whois(dirname);
            directory.Position = 0;
            buffer = new byte[directory.Length];
            directory.Read(buffer, 0, buffer.Length);
            directory.Position = 0;
            foreach (var end in dirEnd)
            {
                TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                if (!client.StoreFile(buffer, dirname))
                    return "552 Requested file action aborted.";
            }

            UnlockDirectory(dirname);


            LogEntry logEntry = new LogEntry
            {
                Date = DateTime.Now,
                CIP = _clientIP,
                CSMethod = "STOR",
                CSUsername = _username,
                SCStatus = "226",
                CSBytes = bytes.ToString()
            };



            return "226 Closing data connection, file transfer successful";
        }

        private bool UnlockDirectory(string pathname)
        {
            string directoryname = GetDirectoryName(pathname);
            List<IPEndPoint> ends = Whois(directoryname);
            int i = 0;
            while(i<ends.Count)
            {
                TCP_Client client = new TCP_Client(new TcpClient(ends[i].Address.ToString(), ends[i].Port));
                if (!client.UnlockFile(directoryname, ID()))
                    return false;
                client.Dispose();
                i++;
            }
            return true;
        }

        private byte[] ID()
        {
            return Chord.ID;
        }

        private string GetDirectoryName(string pathname)
        {
            string[] parts = pathname.Split('\\');
            string name = parts[0];
            if (parts[parts.Length - 1] == "infod")
                return pathname;
            for (int i = 1; i < parts.Length; i++)
                name += @"\" + parts[i];
            name += @"\infod";
            return name;
        }

        private bool LockDirectory(string pathname, bool IsRead= true)
        {
            string directoryname = GetDirectoryName(pathname);
            List<IPEndPoint> ends = Whois(directoryname);
            int i = 0;
            while (i < ends.Count)
            {
                TCP_Client client = new TCP_Client(new TcpClient(ends[i].Address.ToString(), ends[i].Port));
                if (client.LockFile(directoryname, ID(), IsRead))
                {
                    client.Dispose();
                    return true;
                }
                client.Dispose();
                i++;
            }
            return false;
        }

        private bool GetDirectory(string pathname, bool IsDirectory=true)
        {
            string directoryname = GetDirectoryName(pathname);
            if (!IsDirectory)
                directoryname = ParentDirectory(pathname);
            List<IPEndPoint> ends = Whois(directoryname);
            int i = 0;
            while (i < ends.Count)
            {
                TCP_Client client = new TCP_Client(new TcpClient(ends[i].Address.ToString(), ends[i].Port));
                byte[] bytes = client.SearchFile(directoryname);
                if (bytes.Length!=0)
                {
                    try
                    {
                        using (FileStream directory = new FileStream(directoryname, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                        {
                            directory.Write(bytes, 0, bytes.Length);
                        }
                        client.Dispose();
                        return true;
                    }
                    catch
                    {
                        client.Dispose();
                        i++;
                        continue;
                    }
                }
                i++;    
                client.Dispose();
            }
            return false;
        }


        private string AppendOperation(NetworkStream dataStream, string pathname)
        {
            long bytes = 0;
            if (!GetDirectory(pathname,false))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            string descname = "";
            List<MyDirectory> directories = new List<MyDirectory>();
            string directoryname = GetDirectoryName(pathname);
            XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            directories = serializer2.Deserialize(new StreamReader(directoryname)) as List<MyDirectory>;
            foreach (var b in directories.Where(i => i.Name == pathname))
                descname = b.Realname + ".info";
            if (!GetDescriptor(descname))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            if (!LockDescriptor(descname, false))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            List<MyFileDescriptor> files = new List<MyFileDescriptor>();
            XmlSerializer serializer = new XmlSerializer(files.GetType(), new XmlRootAttribute("MyFileDescriptor"));
            Tuple<byte[], FileStream> tuple = null;
            FileStream fs;
            bool LoCogiDeAllaArriba = false;
            if (TCP_Client.PathaIDFS.TryGetValue(pathname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
            {
                fs = tuple.Item2;
                LoCogiDeAllaArriba = true;
            }
            else
                fs = new FileStream(descname, FileMode.Open, FileAccess.Read, FileShare.Read);
            fs.Position = 0;
            files = serializer.Deserialize(new StreamReader(fs)) as List<MyFileDescriptor>;
            if (!LoCogiDeAllaArriba)
                fs.Dispose();
            long size = files[0].Size;
            long parts = files[0].Parts;
            string filename = files[0].Name;
            List<IPEndPoint> ends = Whois(filename + ".part" + (parts - 1));
            for (int j = 0; j < ends.Count; j++)
            {
                IPEndPoint end = ends[j];
                TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                byte[] pbytes = client.SearchFile(filename + ".part" + (parts - 1));
                if (pbytes.Length != 0)
                {
                    if (_connectionType == TransferType.Image)
                    {
                        byte[] part = new byte[1 << 18];
                        for (int i = 0; i < pbytes.Length; i++)
                        {
                            part[i] = pbytes[i];
                        }
                        dataStream.Read(pbytes, pbytes.Length, 1 << 18 - pbytes.Length);
                        bytes += 1 << 18 - pbytes.Length;
                        foreach (var end1 in ends)
                        {
                            TCP_Client client1 = new TCP_Client(new TcpClient(end1.Address.ToString(), end1.Port));
                            if (!client1.StoreFile(pbytes, filename + ".part" + (parts - 1)))
                                return "552 Requested file action aborted.";
                        }
                        break;
                    }
                    else
                    {
                        char[] part = new char[1 << 18];
                        char[] chbytes = Encoding.ASCII.GetChars(pbytes);
                        for (int i = 0; i < chbytes.Length; i++)
                        {
                            part[i] = chbytes[i];
                        }
                        using (StreamReader rdr = new StreamReader(dataStream, Encoding.ASCII))
                        {
                            rdr.ReadBlock(part, chbytes.Length, 1 << 18 - chbytes.Length);
                            bytes += 1 << 18 - chbytes.Length;
                        }
                        foreach (var end1 in ends)
                        {
                            TCP_Client client1 = new TCP_Client(new TcpClient(end1.Address.ToString(), end1.Port));
                            if (!client1.StoreFile(chbytes, filename + ".part" + (parts - 1)))
                                return "552 Requested file action aborted.";
                        }
                        break;
                    }
                }
                else if (j == ends.Count - 1)
                    return "552 Requested file action aborted.";
            }
            using (fs = new FileStream(pathname, FileMode.Append, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan))
            {
                bytes += StoreStream(dataStream, filename, 0, parts);
                if (bytes == 0)
                    return "552 Requested file action aborted.";
            }
            files[0].Size = files[0].Size + bytes;
            files[0].Parts = files[0].Parts + ((size % (1 << 18) == 0) ? size / (1 << 18) : size / (1 << 18) + 1);
            StreamWriter w = new StreamWriter(descname);
            w.BaseStream.SetLength(0);
            w.BaseStream.Flush();
            serializer.Serialize(w, files);
            w.Dispose();
            using (FileStream desc = new FileStream(descname, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                List<IPEndPoint> ends3 = Whois(descname);

                foreach (var end in ends3)
                {
                    TCP_Client client = new TCP_Client(new TcpClient(end.Address.ToString(), end.Port));
                    if (!client.StoreFile(desc, descname))
                        return "552 Requested file action aborted.";
                }
            }
            if(!UnlockDescriptor(descname))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            if(!GetDirectory(pathname))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            if (!LockDirectory(pathname, false))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            directories = new List<MyDirectory>();
            directoryname = GetDirectoryName(pathname);
            XmlSerializer serializer3 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            directories = serializer3.Deserialize(new StreamReader(directoryname)) as List<MyDirectory>;
            foreach (var a in directories.Where(i => i.Name == pathname))
                a.Size = files[0].Size;
            StreamWriter q = new StreamWriter(directoryname);
            q.BaseStream.SetLength(0);
            q.BaseStream.Flush();
            serializer.Serialize(q, directories);
            q.Dispose();
            List < IPEndPoint> ends2 = Whois(directoryname);
            tuple = null;
            FileStream dir;
            if (TCP_Client.PathaIDFS.TryGetValue(pathname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                dir = tuple.Item2;
            else
                dir = new FileStream(directoryname, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            dir.Position = 0;
                foreach (var end2 in ends)
                {
                    TCP_Client client2 = new TCP_Client(new TcpClient(end2.Address.ToString(), end2.Port));
                    if (!client2.StoreFile(dir, directoryname))
                        return "552 Requested file action aborted.";
                }
            
            UnlockDirectory(directoryname);
            LogEntry logEntry = new LogEntry
        {
            Date = DateTime.Now,
            CIP = _clientIP,
            CSMethod = "APPE",
            CSUsername = _username,
            SCStatus = "226",
            CSBytes = bytes.ToString()
        };

            

            return "226 Closing data connection, file transfer successful";
        }


    private bool LockDescriptor(string pathname, bool IsRead = true)
        {
            List<IPEndPoint> ends = Whois(pathname);
            int i = 0;
            while (i < ends.Count)
            {
                TCP_Client client = new TCP_Client(new TcpClient(ends[i].Address.ToString(), ends[i].Port));
                if (client.LockFile(pathname, ID(), IsRead))
                {
                    client.Dispose();
                    return true;
                }
                client.Dispose();
                i++;
            }
            return false;
        }
        private bool UnlockDescriptor(string pathname)
        {
            List<IPEndPoint> ends = Whois(pathname);
            int i = 0;
            while (i < ends.Count)
            {
                TCP_Client client = new TCP_Client(new TcpClient(ends[i].Address.ToString(), ends[i].Port));
                if (!client.UnlockFile(pathname, ID()))
                {
                    client.Dispose();
                    return false;
                }
                client.Dispose();
                i++;
            }
            return true;
        }
        private bool GetDescriptor(string pathname)
        {
            List<IPEndPoint> ends = Whois(pathname);
            int i = 0;
            while (i < ends.Count)
            {
                TCP_Client client = new TCP_Client(new TcpClient(ends[i].Address.ToString(), ends[i].Port));
                byte[] bytes = client.SearchFile(pathname);
                if (bytes.Length != 0)
                {
                    try
                    {
                        using (FileStream directory = new FileStream(pathname, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                        {
                            directory.Write(bytes, 0, bytes.Length);
                        }
                        client.Dispose();
                        return true;
                    }
                    catch
                    {
                        client.Dispose();
                        i++;
                        continue;
                    }
                }
                i++;
                client.Dispose();
            }
            return false;
        }

        
        private string ListOperation(NetworkStream dataStream, string pathname)
        {
            if (!GetDirectory(pathname))
            {
                return "553 Requested action not taken. File name not allowed.";
            }
            List<MyDirectory> directories = new List<MyDirectory>();
            string directoryname = GetDirectoryName(pathname);
            XmlSerializer serializer2 = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            StreamReader a = new StreamReader(directoryname);
            a.BaseStream.Position = 0;
            directories = serializer2.Deserialize(a) as List<MyDirectory>;
            a.Dispose();
            StreamWriter dataWriter = new StreamWriter(dataStream, Encoding.ASCII);
            foreach (var dir in directories.Where(i=>!i.IsFile))
            {
                string date = dir.Date < DateTime.Now - TimeSpan.FromDays(180) ?
                    dir.Date.ToString("MMM dd  yyyy") :
                    dir.Date.ToString("MMM dd HH:mm");
                string[] temp = dir.Name.Split('\\');
                string line = string.Format("drwxr-xr-x    2 2003     2003     {0,8} {1} {2}", "4096", date, temp[temp.Length-1]);

                dataWriter.WriteLine(line);
                dataWriter.Flush();
            }


            foreach (var f in directories.Where(i => i.IsFile))
            {
                string[] temp = f.Name.Split('\\');
                string date = f.Date < DateTime.Now - TimeSpan.FromDays(180) ?
                    f.Date.ToString("MMM dd  yyyy") :
                    f.Date.ToString("MMM dd HH:mm");

                string line = string.Format("-rw-r--r--    2 2003     2003     {0,8} {1} {2}", f.Size, date, temp[temp.Length - 1]);

                dataWriter.WriteLine(line);
                dataWriter.Flush();
            }
            LogEntry logEntry = new LogEntry
            {
                Date = DateTime.Now,
                CIP = _clientIP,
                CSMethod = "LIST",
                CSUsername = _username,
                SCStatus = "226"
            };

           

            return "226 Transfer complete";
        }

        #endregion

        #region IDisposable

                public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_controlClient != null)
                    {
                        _controlClient.Close();
                    }

                    if (_dataClient != null)
                    {
                        _dataClient.Close();
                    }

                    if (_controlStream != null)
                    {
                        _controlStream.Close();
                    }

                    if (_controlReader != null)
                    {
                        _controlReader.Close();
                    }

                    if (_controlWriter != null)
                    {
                        _controlWriter.Close();
                    }
                }
            }

            _disposed = true;
        }
        
        #endregion
    }
}