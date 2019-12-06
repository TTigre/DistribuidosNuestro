using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Sockets;
using System.IO;
using System.Net;
using SharpFtpServer;
using System.Xml.Serialization;
using System.Collections.Concurrent;
using Proyecto_de_Distribuidos_01;

namespace ConsoleApp8
{
    class TCP_Client:IDisposable
    {
        IPEndPoint _remoteEndPoint;
        NetworkStream _controlStream;
        BinaryReader _controlReader;
        BinaryWriter _controlWriter;
        TcpClient client;
        bool disposed = false;
        public TCP_Client(TcpClient client)
        {
            this.client = client;
            _remoteEndPoint = (IPEndPoint)client.Client.RemoteEndPoint;

            _controlStream = client.GetStream();

            _controlReader = new BinaryReader(_controlStream);
            _controlWriter = new BinaryWriter(_controlStream);
        }
        public byte[] SearchFile(string name)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write(bytes.Length + 5);
            _controlWriter.Write((byte)0);
            _controlWriter.Write(bytes, 0, bytes.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if(instruction==7)
            {
                int namelength = _controlReader.ReadInt32();
                byte[] file = _controlReader.ReadBytes(namelength);
                string filename = Encoding.UTF8.GetString(file);
                if(filename==name)
                {
                    return _controlReader.ReadBytes(length - 5 - namelength);
                }
            }
            return new byte[0];

        }
        public bool DeleteFile(string name)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write(bytes.Length + 5);
            _controlWriter.Write((byte)5);
            _controlWriter.Write(bytes, 0, bytes.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if (instruction == 3)
            {
                byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                string answer = Encoding.UTF8.GetString(bytes2);
                string resp = answer.Substring(0, 2);
                string filename = answer.Substring(2, answer.Length - 2);
                if (resp == "SI" && filename == name)
                    return true;
            }
            return false;
        }

        public bool LockFile(string name, byte[] sha, bool IsRead=true)
        {
            byte type = 1;
            if (!IsRead)
                type = 9;
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write(bytes.Length + 21);
            _controlWriter.Write(type);
            _controlWriter.Write(sha);
            _controlWriter.Write(bytes, 0, bytes.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if (instruction == 3)
            {
                byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                string answer = Encoding.UTF8.GetString(bytes2);
                string resp = answer.Substring(0, 2);
                string filename = answer.Substring(2, answer.Length - 2);
                if (resp == "SI" && filename == name)
                    return true;
            }
            return false;
        }
        public bool UnlockFile(string name, byte[] sha)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write(bytes.Length + 21);
            _controlWriter.Write(8);
            _controlWriter.Write(sha);
            _controlWriter.Write(bytes, 0, bytes.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if (instruction == 3)
            {
                byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                string answer = Encoding.UTF8.GetString(bytes2);
                string resp = answer.Substring(0, 2);
                string filename = answer.Substring(2, answer.Length - 2);
                if (resp == "SI" && filename == name)
                    return true;
            }
            return false;
        }
        public bool StoreFile(byte[] file, string name)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write(bytes.Length + 9 + file.Length);
            _controlWriter.Write((byte)4);
            _controlWriter.Write(bytes.Length);
            _controlWriter.Write(bytes, 0, bytes.Length);
            _controlWriter.Write(file, 0, file.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if (instruction == 3)
            {
                byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                string answer = Encoding.UTF8.GetString(bytes2);
                string resp = answer.Substring(0, 2);
                string filename = answer.Substring(2, answer.Length - 2);
                if (resp == "SI" && filename == name)
                    return true;
            }
            return false;
        }
        public bool StoreFile(char[] file, string name)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write(bytes.Length + 9 + file.Length);
            _controlWriter.Write((byte)4);
            _controlWriter.Write(bytes.Length);
            _controlWriter.Write(bytes, 0, bytes.Length);
            _controlWriter.Write(file, 0, file.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if (instruction == 3)
            {
                byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                string answer = Encoding.UTF8.GetString(bytes2);
                string resp = answer.Substring(0, 2);
                string filename = answer.Substring(2, answer.Length - 2);
                if (resp == "SI" && filename == name)
                    return true;
            }
            return false;
        }
        public bool StoreFile(Stream file, string name)
        {
            byte[] buffer = new byte[file.Length];
            file.Read(buffer, 0, buffer.Length);
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            _controlWriter.Write((int)(bytes.Length + 9 + file.Length));
            _controlWriter.Write((byte)4);
            _controlWriter.Write(bytes.Length);
            _controlWriter.Write(bytes, 0, bytes.Length);
            _controlWriter.Write(buffer, 0, buffer.Length);
            int length = _controlReader.ReadInt32() - 4;
            byte instruction = _controlReader.ReadByte();
            if (instruction == 3)
            {
                byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                string answer = Encoding.UTF8.GetString(bytes2);
                string resp = answer.Substring(0, 2);
                string filename = answer.Substring(2, answer.Length - 2);
                if (resp == "SI" && filename == name)
                    return true;
            }
            return false;
        }
        public void Answer(object obj)
        { 
            string _clientIP = _remoteEndPoint.Address.ToString();
            int length = _controlReader.ReadInt32() - 4;
            byte[] instruction = _controlReader.ReadBytes(length);
            byte[] answers = Response(instruction, length);
            //_controlWriter.Write(answers);
        }
        public byte[] Response(byte[] instruction, int Length)
        {
            int Type = instruction[0];
            byte[] Data = instruction;
            switch (Type)
            {
                case 0:
                    string name = Encoding.UTF8.GetString(Data, 1, Data.Length - 1);
                    return Read(name);
                case 1:
                    byte[] SHA1 = new byte[16];
                    for (int i = 1; i < 17; i++)
                        SHA1[i-1] = Data[i];
                    name = Encoding.UTF8.GetString(Data, 17, Data.Length - 17);
                    return BlockResourceRead(SHA1, name);
                case 2:
                    byte[] origin = new byte[16];
                    for (int i = 1; i < 17; i++)
                        origin[i - 1] = Data[i];
                    byte[] solicitant = new byte[16];
                    for (int i = 17; i < 33; i++)
                        solicitant[i-17]= Data[i];
                    name = Encoding.UTF8.GetString(Data, 33, Data.Length - 33);
                    return BlockResourceRead(origin, solicitant, name);
                case 3:
                    string answer = Encoding.UTF8.GetString(Data, 1, Data.Length - 1);
                    return Response(answer);
                case 4:
                    int length = Data[1] + (Data[2] << 8) + (Data[3] << 16) + (Data[4] << 24);
                    name = Encoding.UTF8.GetString(Data, 5, length);
                    return Store(name, length, Data);
                case 5:
                    name = Encoding.UTF8.GetString(Data, 1, Data.Length - 1);
                    return Delete(name);
                case 6:
                    int type = Data[1];
                    return CompletedInstruction(type);
                case 7:
                    length = Data[1] + (Data[2] << 8) + (Data[3] << 16) + (Data[4] << 24);
                    name = Encoding.UTF8.GetString(Data, 5, length);
                    return Recieve(name, length, Data);
                case 8:
                    SHA1 = new byte[16];
                    for (int i = 1; i < 17; i++)
                        SHA1[i - 1] = Data[i];
                    name = Encoding.UTF8.GetString(Data, 17, Data.Length - 17);
                    return UnblockResource(SHA1, name);
                case 9:
                    SHA1 = new byte[16];
                    for (int i = 1; i < 17; i++)
                        SHA1[i - 1] = Data[i];
                    name = Encoding.UTF8.GetString(Data, 17, Data.Length - 17);
                    return BlockResourceRead(SHA1, name,false);
                case 10:
                    origin = new byte[16];
                    for (int i = 1; i < 17; i++)
                        origin[i - 1] = Data[i];
                    solicitant = new byte[16];
                    for (int i = 17; i < 33; i++)
                        solicitant[i - 1] = Data[i];
                    name = Encoding.UTF8.GetString(Data, 33, Data.Length - 33);
                    return BlockResourceRead(origin, solicitant, name,false);
                default:
                    break;
            }
            return new byte[0];
        }
        private byte[] UnblockResource(byte[] sha1, string name)
        {
            
               if(PathaIDFS.TryRemove(name,out Tuple<byte[], FileStream> tuple))
            {
                tuple.Item2.Dispose();
                string s1 = "SI" + name;
                var temp1 = Encoding.UTF8.GetBytes(s1);
                _controlWriter.Write(5 + temp1.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp1);
                return new byte[0];
            }
            string s2 = "NO" + name;
            var temp2 = Encoding.UTF8.GetBytes(s2);
            _controlWriter.Write(5 + temp2.Length);
            _controlWriter.Write((byte)3);
            _controlWriter.Write(temp2);
            return new byte[0];
        }

        private byte[] Recieve(string pathname, int length, byte[] data)
        {
            try
            {
                Directory.CreateDirectory(pathname);
                FileStream fs = PathaIDFS[pathname].Item2;
                fs.Position = 0;
                CopyStream(data, fs, length + 5);
                string s2 = "SI" + pathname;
                var temp2 = Encoding.UTF8.GetBytes(s2);
                _controlWriter.Write(5 + temp2.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp2);
            }
            catch
            {
                string s2 = "NO" + pathname;
                var temp2 = Encoding.UTF8.GetBytes(s2);
                _controlWriter.Write(5 + temp2.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp2);

            }
            return new byte[0];
        }

        private byte[] CompletedInstruction(int type)
        {
            throw new NotImplementedException();
        }

        private byte[] Delete(string name)
        {
            if (PathaIDFS.TryRemove(name, out Tuple<byte[], FileStream> tuple))
            {
                tuple.Item2.Dispose();
                File.Delete(name);
                string s1 = "SI" + name;
                var temp1 = Encoding.UTF8.GetBytes(s1);
                _controlWriter.Write(5 + temp1.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp1);
                return new byte[0];
            }
            string s2 = "NO" + name;
            var temp2 = Encoding.UTF8.GetBytes(s2);
            _controlWriter.Write(5 + temp2.Length);
            _controlWriter.Write((byte)3);
            _controlWriter.Write(temp2);
            return new byte[0];
        }

        private byte[] Store(string pathname, int length, byte[] data)
        {
            try
            {
                var b = pathname.Split('\\');
                string directory = b[0];
                for (int i = 1; i < b.Length-1; i++)
                    directory += @"\" + b[i];
                Directory.CreateDirectory(directory);
                Tuple<byte[], FileStream> tuple;
                FileStream fs;
                if (PathaIDFS.TryGetValue(pathname, out tuple) && Chord.comp.Equals(tuple.Item1, Chord.ID))
                    fs = tuple.Item2;
                else
                    fs = new FileStream(pathname, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                fs.Position = 0;
                CopyStream(data, fs, length + 5);
                fs.Dispose();
                string s2 = "SI" + pathname;
                var temp2 = Encoding.UTF8.GetBytes(s2);
                _controlWriter.Write(5 + temp2.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp2);
            }
            catch
            {
                string s2 = "NO" + pathname;
                var temp2 = Encoding.UTF8.GetBytes(s2);
                _controlWriter.Write(5 + temp2.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp2);

            }
            return new byte[0];
        }

        private byte[] Response(string answer)
        {
            throw new NotImplementedException();
        }

        //Falta cambiar el comparador por defecto de byte[]
        public static ConcurrentDictionary<string,Tuple<byte[],FileStream>> PathaIDFS = new ConcurrentDictionary<string, Tuple<byte[], FileStream>>();
        static int replicacion = 1;

        private byte[] BlockResourceRead(byte[] origin, byte[] solicitant, string name, bool isRead=true)
        {
            FileStream fs = null;
            try
            {
                
                if (PathaIDFS.ContainsKey(name) && Chord.comp.Compare(solicitant,PathaIDFS[name].Item1)==1)////// Ojoooooooooo hay que copiar el comparador
                {
                    goto no;
                }
                FileMode modo = FileMode.Open;
                FileAccess acceso = FileAccess.Read;
                FileShare share = FileShare.Read;
                
                if (!isRead)
                {
                    modo = FileMode.OpenOrCreate;
                    acceso = FileAccess.ReadWrite;
                    share = FileShare.None;
                }

                fs = new FileStream(name, modo, acceso, share);
                PathaIDFS[name] = new Tuple<byte[], FileStream>(origin, fs);
                string s2 = "SI" + name;
                var temp2 = Encoding.UTF8.GetBytes(s2);
                _controlWriter.Write(5 + temp2.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp2);
                return new byte[0];

            no:
                string s = "NO" + name;
                var temp = Encoding.UTF8.GetBytes(s);
                _controlWriter.Write(5 + temp.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp);
                if (fs != null)
                    fs.Dispose();
                return new byte[0];
            }
            catch
            {
                string s = "NO" + name;
                var temp = Encoding.UTF8.GetBytes(s);
                _controlWriter.Write(5 + temp.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp);
                return new byte[0];
            }
            throw new NotImplementedException();
        }

        private List<IPEndPoint> WHOIS(List<byte[]> ID)
        {
            throw new NotImplementedException();
        }
        static byte[] ID { get { return Chord.ID; } }
        private byte[] HasheadorRico(byte[] ar)
        {
            throw new NotImplementedException();
        }
        public List<byte[]> ListarReplicados(byte[] sha)
        {
            List<byte[]> vecinos = new List<byte[]>();
            for (int i = 1; i < 1 << replicacion; i++)
            {
                byte[] temp = sha.Clone() as byte[];
                temp[0] += (byte)(i << (8 - replicacion));
                vecinos.Add(temp);
            }
            return vecinos;
        }


        private byte[] BlockResourceRead(byte[] sha1, string name, bool IsRead=true)
        {
            try
            {
                List<byte[]> response = new List<byte[]>();
                FileStream fs = null;
                if(PathaIDFS.ContainsKey(name))
                {
                    goto no;
                }
                FileMode modo = FileMode.Open;
                FileAccess acceso = FileAccess.Read;
                FileShare share = FileShare.Read;
                if(!IsRead)
                {
                    modo = FileMode.OpenOrCreate;
                    acceso = FileAccess.ReadWrite;
                    share = FileShare.None;
                }
                fs = new FileStream(name, modo, acceso, share);
                PathaIDFS[name] = new Tuple<byte[], FileStream>(sha1, fs);
                var receivers = Chord.WHOISTHIS(name);
                byte[] bytes = Encoding.UTF8.GetBytes(name);
                byte[] respuestica = new byte[bytes.Length + 32];
   
                for(int i=0;i<16;i++)
                {
                    respuestica[i] = sha1[i];
                    respuestica[i + 16] = ID[i];
                }
                for(int i=0; i<bytes.Length;i++)
                {
                    respuestica[i + 32] = bytes[i];
                }
                for(int i=0;i<1<<replicacion;i++)
                {
                    if (Chord.comp.Equals(new IPEndPoint(IPAddress.Parse("127.0.0.1"), Chord.port), receivers[i]))
                        continue;
                    TcpClient a = new TcpClient();
                    a.Connect(receivers[i]);
                    var stream=a.GetStream();
                    var bin = new BinaryWriter(stream);
                    bin.Write(respuestica.Length + 5);
                    bin.Write((byte)2);
                    bin.Write(respuestica);
                    var binRes = new BinaryReader(stream);
                    int length = binRes.ReadInt32() - 4;
                    byte instruction = binRes.ReadByte();
                    if (instruction == 3)
                    {
                        byte[] bytes2 = _controlReader.ReadBytes(length - 1);
                        string answer = Encoding.UTF8.GetString(bytes2);
                        string resp = answer.Substring(0, 2);
                        string filename = answer.Substring(2, answer.Length - 2);
                        if (resp == "NO" || filename != name)
                        {
                            goto no;
                        }

                    }
                    

                }
                string s2 = "SI" + name;
                var temp2 = Encoding.UTF8.GetBytes(s2);
                _controlWriter.Write(5 + temp2.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp2);
                return new byte[0];
            no:
                string s = "NO" + name;
                var temp = Encoding.UTF8.GetBytes(s);
                _controlWriter.Write(5 + temp.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp);
                if(fs!=null)
                    fs.Dispose();
                return new byte[0];
            }
            catch
            {
                string s = "NO" + name;
                var temp = Encoding.UTF8.GetBytes(s);
                _controlWriter.Write(5 + temp.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(temp);
                return new byte[0];
            }
        }

        private byte[] Read(string name)
        {
            try
            {
                FileStream fs = new FileStream(name, FileMode.Open, FileAccess.Read, FileShare.Read);
                byte[] bytes = Encoding.UTF8.GetBytes(name);
                _controlWriter.Write((int)fs.Length + 9 + bytes.Length);
                _controlWriter.Write((byte)7);
                _controlWriter.Write(bytes.Length);
                _controlWriter.Write(bytes);
                SharpFtpServer.ClientConnection.CopyStream(fs, _controlWriter.BaseStream, 4096);
                fs.Dispose();
            }
            catch
            {
                byte[] bytes = Encoding.UTF8.GetBytes("NO"+name);
                _controlWriter.Write(5 + bytes.Length);
                _controlWriter.Write((byte)3);
                _controlWriter.Write(bytes);
            }
            return new byte[0];
        }

        private void CopyStream(byte[] buffer, Stream output, int offset)
        {

            output.Write(buffer, offset, buffer.Length - offset);
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
                    if (client != null)
                    {
                        client.Close();
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

            disposed = true;
        }
    }
}
