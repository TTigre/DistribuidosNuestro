using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Collections.Concurrent;

namespace Proyecto_de_Distribuidos_01
{
    static class InstructionHandler
    {
        static SHA1Managed Hasher = new SHA1Managed();
    }
    class Comparador : IComparer<byte[]>,IEqualityComparer<byte[]>,IEqualityComparer<Paquete>,IEqualityComparer<IPEndPoint>
    {
        public int Compare(byte[] x, byte[] y)
        {
            int min = Math.Min(x.Length, y.Length);
            for(int i=0; i<min; i++)
            {
                if (x[i] == y[i])
                    continue;
                return x[i]>y[i]?1:-1;
            }
            return 0;
        }

        public bool Equals(byte[] x, byte[] y)
        {
            return Compare(x, y) == 0;
        }

        public bool Equals(Paquete x, Paquete y)
        {
            if(x.tipo!=5)
                return x.CompareTo(y)==0;
            else
            {
                if (x.tipo != y.tipo|| !Equals(x.DestinouOrigen, y.DestinouOrigen))
                    return false;
                byte[] sha1 = new byte[16];
                byte[] sha2 = new byte[16];
                for (int i = 0; i < 16; i++)
                {
                    if (x.Mensaje[i + 9] != y.Mensaje[i + 9])
                        return false;
                }
                return true;
            }
        }

        public bool Equals(IPEndPoint x, IPEndPoint y)
        {
            return Equals(x.Address.GetAddressBytes(), y.Address.GetAddressBytes()) && x.Port == y.Port;
        }

        public int GetHashCode(byte[] obj)
        {
            int acum = 0;
            for(int i=0; i<obj.Length; i++)
            {
                acum += ((int)obj[i]) << (i % 4);
            }
            return acum;
        }

        public int GetHashCode(Paquete obj)
        {

            int acum=GetHashCode(obj.DestinouOrigen.Address.GetAddressBytes())+obj.DestinouOrigen.Port;
            if(obj.tipo!=5)
                acum += GetHashCode(obj.ID);
            else
            {
                byte[] sha = new byte[16];
                for (int i = 0; i < 16; i++)
                    sha[i] = obj.Mensaje[i + 9];
                acum += GetHashCode(sha);
            }
            return acum;
        }

        public int GetHashCode(IPEndPoint obj)
        {
            return GetHashCode(obj.Address.GetAddressBytes()) + obj.Port;
        }
    }
    public class Paquete:IComparable<Paquete>
    {
        public Stopwatch stopwatch=new Stopwatch();
        public byte[] Mensaje;
        public byte[] ID;
        public IPEndPoint DestinouOrigen;
        public int Tries;
        public int tipo;
        public long TiempoEsperado
        { get
            {
                return stopwatch.ElapsedMilliseconds;
            }
        }
        public Paquete(byte[] mensaje, IPEndPoint destino)
        {
            Stopwatch stopwatch = new Stopwatch();
            Tries = 0;
            Mensaje = mensaje;
            DestinouOrigen = destino;
            ID = new byte[8];
            if(mensaje[0]!=6)
            for(int i=0; i<8;i++)
            {
                ID[i] = Mensaje[i + 1];
            }
            tipo = Mensaje[0];
        }
        public Paquete(UdpReceiveResult mensaje):this(mensaje.Buffer,mensaje.RemoteEndPoint)
        {
            ;
        }

        public int CompareTo(Paquete other)
        {
            Comparador comparador = new Comparador();
            int comparacionInicial = comparador.Compare(this.DestinouOrigen.Address.GetAddressBytes(), other.DestinouOrigen.Address.GetAddressBytes());
            if(comparacionInicial==0)
            {
                comparacionInicial = this.DestinouOrigen.Port.CompareTo(other.DestinouOrigen.Port);
                if(comparacionInicial==0)
                {
                    return comparador.Compare(ID, other.ID);
                }
            }
            return comparacionInicial;
        }
    }

    class Chord
    {
        public static bool SoyElPrimero = false;
        static HashSet<Task> Tareas = new HashSet<Task>();
        static Dictionary<Task, Task> RelacionDelayTarea = new Dictionary<Task, Task>();
        static Dictionary<Task, Task> RelacionTareaDelay = new Dictionary<Task, Task>();
        static Dictionary<IPEndPoint, byte[]> ShasNuevos = new Dictionary<IPEndPoint, byte[]>();
        public static Comparador comp = new Comparador();
        static UdpClient udp;
        static TcpClient mitcp;
        static SHA1Managed Hasher = new SHA1Managed();
        public static byte[] ID;
        static byte[] VecinoSup;
        static byte[] VecinoInf;
        static IPEndPoint VecinoSupIp;
        static IPEndPoint VecinoInfIp;
        public static int port;
        static int Tipo5=0;
        static int ReplicationLevel = 1;
        static Random random = new Random();
        public static byte[] Suma(byte[] first, byte[] second)
        {
            byte[] resultado = new byte[16];
            int carry = 0;
            for (int i = 15; i >= 0; i--)
            {
                int valor = (first[i] + second[i] + carry);
                resultado[i] = (byte)(valor);
                if (valor > byte.MaxValue)
                    carry = 1;
                else
                    carry = 0;
            }
            return resultado;
        }
        public static byte[] Resta(byte[] first, byte[] second)
        {
            byte[] resultado = new byte[16];
            int carry = 0;
            for(int i=15; i>=0;i--)
            {
                int valor = (first[i] - second[i] - carry);
                resultado[i] = (byte)(valor);
                if (valor < 0)
                    carry = 1;
                else
                    carry = 0;
            }
            return resultado;
        }
        public static byte[] Divide(byte[] dividendo, int divisor)
        {
            byte[] cociente = new byte[16];
            int resto = 0;
            for(int i=0;i<16;i++)
            {
                cociente[i] = (byte)((dividendo[i] + (resto << 8)) / divisor);
                resto = dividendo[i] - cociente[i];
            }
            return cociente;
        }
        static long MessageID = 0;
        static object CountLock = new object();
        static ConcurrentDictionary<IPEndPoint, byte[]> ListaDeNuevos = new ConcurrentDictionary<IPEndPoint, byte[]>(comp);
        static ConcurrentDictionary<byte[],IPEndPoint> IDaNuevo = new ConcurrentDictionary<byte[],IPEndPoint>(comp);

        static List<byte[]> ProcesaMensajeConID(UdpReceiveResult mensaje, out List<IPEndPoint> destino, out List<byte[]> IDs)
        {
            if (mensaje.RemoteEndPoint.Port == 7001)
                ;
            byte[] mensajeSinID = null;
            if (mensaje.Buffer[0] != 6)
                 mensajeSinID= new byte[mensaje.Buffer.Length - 8];
            else
                mensajeSinID = new byte[1];
            List<byte[]> finalResponse = new List<byte[]>();
            mensajeSinID[0] = mensaje.Buffer[0];
            byte[] ShaNuevo = null;
            IPEndPoint direccionNuevo = null;
            for(int i=9; i<mensaje.Buffer.Length;i++)
            {
                mensajeSinID[i - 8] = mensaje.Buffer[i];
            }
            //if(mensaje.Buffer[0]==6)
            //{
            //    ListaDeNuevos[mensaje.RemoteEndPoint]=Hasher.ComputeHash(mensajeSinID, 1, 48);
            //}
            if(mensaje.Buffer[0]==1&&ListaDeNuevos.ContainsKey(mensaje.RemoteEndPoint))
            {
                ShaNuevo = ListaDeNuevos[mensaje.RemoteEndPoint];
                mensajeSinID[0] = 20;
            }

        preresponse:

            if (mensaje.Buffer[0] > 1&&mensaje.Buffer[0]!=6&&mensaje.Buffer[0]!=9)
                ;

            var response = ProcesaMensaje(new UdpReceiveResult(mensajeSinID, mensaje.RemoteEndPoint), out destino, out IDs, ShaNuevo, direccionNuevo);

            if(mensajeSinID[0]==4)
            {
                DiccioDeIDaIP[IDs[1]] = destino[0];
                if ( IDaNuevo.ContainsKey(IDs[0]))
                {
                    ShaNuevo = IDs[0];
                    direccionNuevo = IDaNuevo[ShaNuevo];
                    mensajeSinID[0] = 21;
                    goto preresponse;
                }
            }

            foreach (var a in response)
            {
                byte[] NewResponse = new byte[a.Length+8];
                for(int i=1; i<a.Length;i++)
                {
                    NewResponse[i + 8] = a[i];
                }
                NewResponse[0] = a[0];
                Monitor.Enter(CountLock);
                for(int i=0;i<8;i++)
                {
                    NewResponse[i + 1] = (byte)(MessageID >> (i * 8));
                }
                MessageID++;
                Monitor.Exit(CountLock);
                finalResponse.Add(NewResponse);
            }
            if (mensaje.Buffer[0] != 9&&mensaje.Buffer[0]!=6)
            {
                byte[] ACK = new byte[9];
                ACK[0] = 9;
                for (int i = 1; i < ACK.Length; i++)
                {
                    ACK[i] = mensaje.Buffer[i];
                }
                finalResponse.Add(ACK);
                destino.Add(mensaje.RemoteEndPoint);
            }
            return finalResponse;
        }

        //El ACK se debe implementar de forma externa y al recibir el retorno se le debe agregar el ID
        static List<byte[]> ProcesaMensaje(UdpReceiveResult mensaje, out List<IPEndPoint> destino, out List<byte[]> IDs,byte[] ShaNuevo=null, IPEndPoint direccionNuevo=null)
        {
            IDs = new List<byte[]>();
            List<byte[]> response = new List<byte[]>();
            byte[] tempresponse = null;
            destino = new List<IPEndPoint>();
            switch (mensaje.Buffer[0])
            {
                case 0:
                    tempresponse = new byte[33];
                    tempresponse[0] = 1;
                    byte[] hash = Hasher.ComputeHash(mensaje.Buffer, 1, 48);
                    if (ID == null)
                        ID = hash;
                    for (int i = 0; i < 16; i++)
                    {
                        tempresponse[i + 1] = hash[i];
                        tempresponse[i + 17] = ID[i];
                    }
                    response.Add(tempresponse);
                    destino.Add(mensaje.RemoteEndPoint);
                    return response;
                case 1:
                    destino = new List<IPEndPoint>();
                    byte[] id1 = new byte[16];
                    for(int i=0; i<16;i++)
                    {
                        id1[i] = mensaje.Buffer[i + 17];
                    }
                    IDs.Add(id1);
                    return response;
                case 2:
                    Console.WriteLine("Me llego un 2");
                    byte[] key = new byte[16];
                    for(int i=0; i<16; i++)
                    {
                        key[i] = mensaje.Buffer[i + 1];
                    }
                    if(DeQuienEsLaTarea(key)==0)
                    {
                        tempresponse = new byte[37];
                        tempresponse[0] = 4;
                        for(int i=0; i<16; i++)
                        {
                            tempresponse[i + 1] = key[i];
                        }
                        for(int i=0; i<4; i++)
                        {
                            tempresponse[i + 17] = (byte)(Chord.port >> (8 * i));
                        }
                        for (int i = 0; i < 16; i++)
                        {
                            tempresponse[i + 21] = ID[i];
                        }
                        response.Add(tempresponse);
                        destino.Add(mensaje.RemoteEndPoint);
                        return response;
                    }
                    IPEndPoint end = VecinoInfIp;
                    if (DeQuienEsLaTarea(key)==1)
                        end = VecinoSupIp;
                    tempresponse = new byte[41];
                    tempresponse[0] = 3;
                    for (int i = 0; i < 16; i++)
                    {
                        tempresponse[i + 1] = key[i];
                    }
                    var temp = end.Address.GetAddressBytes();
                    for (int i = 0; i < 4; i++)
                    {
                        tempresponse[i + 17] = temp[i];
                        tempresponse[i + 21] = (byte)(end.Port >> (8 * i));
                    }
                    for (int i = 0; i < 16; i++)
                    {
                        tempresponse[i + 25] = ID[i];
                    }
                    response.Add(tempresponse);
                    destino.Add(mensaje.RemoteEndPoint);
                    return response;
                case 3:
                    tempresponse = new byte[17];
                    tempresponse[0] = 2;
                    for(int i=0; i<16; i++)
                    {
                        tempresponse[i + 1] = mensaje.Buffer[i + 1];
                    }
                    response.Add(tempresponse);
                    destino.Add(mensaje.RemoteEndPoint);
                    return response;
                case 4:
                    caso4:
                    byte[] id4 = new byte[16];
                    byte[] whoisResponse = new byte[16];
                    int port = 0;
                    for (int i = 0; i < 16; i++)
                    {
                        id4[i] = mensaje.Buffer[21 + i];
                        whoisResponse[i] = mensaje.Buffer[1 + i];
                    }
                    
                    for(int i=0; i<4; i++)
                    {
                        port+= (mensaje.Buffer[i+17] << (8 * i));
                    }
                    var paraelDic = new IPEndPoint(mensaje.RemoteEndPoint.Address, port);
                    DiccioDeIDaIP[whoisResponse] = paraelDic;
                    destino.Add(paraelDic);
                    IDs.Add(whoisResponse);
                    IDs.Add(id4);
                    return response;
                case 5:
                    Tipo5++;
                    byte[] key2 = new byte[16];
                    tempresponse = new byte[25];
                    for (int i = 0; i < 16; i++)
                    {
                        key2[i] = mensaje.Buffer[i + 1];
                    }
                    string ip = "";
                    int portnum = 0;
                    for (int i = 17; i < 20; i++)
                    {
                        ip += mensaje.Buffer[i] + ".";
                    }
                    ip += mensaje.Buffer[20];
                    for (int i = 0; i < 4; i++)
                    {
                        portnum += mensaje.Buffer[i + 21] << (i * 8);
                    }
                    IPEndPoint nuevo = new IPEndPoint(IPAddress.Parse(ip), portnum);
                    IPEndPoint yo = new IPEndPoint(IPAddress.Parse("127.0.0.1"), Chord.port);
                    if (comp.Equals(nuevo, yo))
                        return response;
                    byte[] nuevoID = Suma(VecinoInf, Divide(Resta(ID, VecinoInf), 2));
                    if (comp.Compare(nuevoID, ID) == 0)
                    {
                        for (int i = 0; i < 16; i++)
                            nuevoID[i] = ID[i];
                        nuevoID[0] += 128;
                    }
                    if(EnRango(key2,VecinoInf,ID)||comp.Equals(VecinoInfIp,yo))
                    {
                        for (int i = 0; i < 16; i++)
                            tempresponse[i + 1] = nuevoID[i];
                        for(int i=0; i<4; i++)
                        {
                            tempresponse[i + 17] = nuevo.Address.GetAddressBytes()[i];
                            tempresponse[i + 21] = (byte)(nuevo.Port >> (8 * i));
                        }
                        tempresponse[0] = 10;
                        if (!comp.Equals(VecinoInfIp, yo))
                        {
                            response.Add(tempresponse);
                            destino.Add(VecinoInfIp);
                        }
                        tempresponse = new byte[65];
                        for (int i = 0; i < 16; i++)
                        {
                            tempresponse[i + 1] = VecinoInf[i];
                            tempresponse[i + 25] = ID[i];
                            tempresponse[i + 49] = nuevoID[i];
                        }
                        for (int i = 0; i < 4; i++)
                        {
                            tempresponse[17 + i] = VecinoInfIp.Address.GetAddressBytes()[i];
                            tempresponse[17 + 8 + 16 + i] = yo.Address.GetAddressBytes()[i];
                            tempresponse[17 + 4 + i] = (byte)(VecinoInfIp.Port >> (i * 8));
                            tempresponse[17 + 8 + 16 + 4 + i] = (byte)(yo.Port >> (i * 8)); ;
                        }
                        tempresponse[0] = 8;
                        response.Add(tempresponse);
                        destino.Add(nuevo);
                        if (comp.Equals(VecinoInfIp, yo))
                        {
                            for(int i=0; i<16; i++)
                            {
                                Chord.VecinoSup[i] = nuevoID[i];
                            }
                            VecinoSupIp = new IPEndPoint(new IPAddress(nuevo.Address.GetAddressBytes()),portnum);
                        }
                        for (int i = 0; i < 16; i++)
                        {
                            Chord.VecinoInf[i] = nuevoID[i];
                        }
                        VecinoInfIp = new IPEndPoint(new IPAddress(nuevo.Address.GetAddressBytes()), portnum);
                    }
                    //if (comp.Equals(nuevo, new IPEndPoint(IPAddress.Parse("127.0.0.1"), Program.port)))
                    //    return response;
                    //byte[] nuevoID = Suma(VecinoInf, Divide(Resta(ID, VecinoInf), 2));
                    //if (EnRango(key2,VecinoInf,ID)||comp.Equals(new IPEndPoint(IPAddress.Parse("127.0.0.1"),Program.port),VecinoInfIp))
                    //{
                    //    //byte[] nuevoID = Suma(VecinoInf,Divide(Resta(ID,VecinoInf),2));
                    //    if(comp.Compare(nuevoID,new byte[0])==0)
                    //    {
                    //        nuevoID[0] += 128;
                    //    }
                    //    if (!comp.Equals(new IPEndPoint(IPAddress.Parse("127.0.0.1"), Program.port), VecinoInfIp))
                    //    {
                    //        destino.Add(VecinoInfIp);

                    //        for (int i = 0; i < mensaje.Buffer.Length; i++)
                    //        {
                    //            tempresponse[i] = mensaje.Buffer[i];
                    //        }
                    //        for(int i=0; i<16; i++)
                    //        {
                    //            tempresponse[i + 1] = nuevoID[i];
                    //        }
                    //        response.Add(tempresponse);
                    //    }
                    //    tempresponse = new byte[65];
                    //    tempresponse[0] = 8;
                    //    for (int i = 0; i < 16; i++)
                    //    {
                    //        tempresponse[i + 1] = VecinoInf[i];
                    //        tempresponse[i + 25] = ID[i];
                    //        tempresponse[i + 49] = nuevoID[i];
                    //    }
                    //    for(int i=0; i<4; i++)
                    //    {
                    //        tempresponse[17 + i]=VecinoInfIp.Address.GetAddressBytes()[i];
                    //        tempresponse[17 + 8 + 16 + i]=VecinoSupIp.Address.GetAddressBytes()[i];
                    //        tempresponse[17 + 4 + i]=(byte)(VecinoInfIp.Port >> (i * 8));
                    //        tempresponse[17 + 8 + 16 + 4 + i] = (byte)(VecinoSupIp.Port >> (i * 8)); ;
                    //    }

                    //    response.Add(tempresponse);
                    //    destino.Add(nuevo);
                    //    VecinoInf = nuevoID;
                    //    VecinoInfIp = nuevo;
                    //}
                    ////else if((comp.Compare(ID, VecinoInf) >= 0 && (comp.Compare(key2, ID) <= 0 && comp.Compare(VecinoInf, key2) <= 0)))
                    //else
                    //{
                    //    VecinoSup = key2;
                    //    VecinoSupIp = nuevo;
                    //}
                    //if (comp.Equals(new IPEndPoint(IPAddress.Parse("127.0.0.1"), Program.port), VecinoSupIp))
                    //{
                    //    VecinoSup = nuevoID;
                    //    VecinoSupIp = nuevo;
                    //}
                    return response;
                case 6:
                    Console.WriteLine("Me llego un 6");
                    byte[] cadenainicial = new byte[49];
                    random.NextBytes(cadenainicial);
                    cadenainicial[0] = 0;
                    response.Add(cadenainicial);
                    destino.Add(mensaje.RemoteEndPoint);
                    IDs.Add(Hasher.ComputeHash(cadenainicial, 1, 48));
                    ListaDeNuevos[mensaje.RemoteEndPoint] = IDs[IDs.Count-1];
                    return response;
                case 7:
                case 8:
                    if(VecinoInf==null)
                    {
                        VecinoInf = new byte[16];
                        VecinoSup = new byte[16];
                        ID = new byte[16];
                        for(int i=0; i<16; i++)
                        {
                            VecinoInf[i] = mensaje.Buffer[1 + i];
                            VecinoSup[i] = mensaje.Buffer[17 + 8 + i];
                            ID[i] = mensaje.Buffer[49 + i];
                        }
                        byte[] ip1 = new byte[4];
                        int port1 = 0;
                        byte[] ip2 = new byte[4];
                        int port2 = 0;
                        for(int i=0;i<4;i++)
                        {
                            ip1[i] = mensaje.Buffer[17 + i];
                            ip2[i] = mensaje.Buffer[17 + 8 + 16 + i];
                            port1 += mensaje.Buffer[17 + 4 + i] << (i * 8);
                            port2 += mensaje.Buffer[17 + 8+16+4 + i] << (i * 8);
                        }
                        VecinoInfIp = new IPEndPoint(new IPAddress(ip1), port1);
                        VecinoSupIp = new IPEndPoint(new IPAddress(ip2), port2);
                        if (!comp.Equals(VecinoSupIp, mensaje.RemoteEndPoint))
                        {
                            if (comp.Equals(VecinoInfIp, VecinoSupIp))
                                VecinoInfIp = mensaje.RemoteEndPoint;
                            VecinoSupIp = mensaje.RemoteEndPoint;
                            
                        }
                    }
                    return response;
                case 10:
                    for(int i=0;i<16;i++)
                    {
                        VecinoSup[i] = mensaje.Buffer[i + 1];
                    }
                    byte[] ipderecho = new byte[4];
                    int portderecho = 0;
                    for (int i = 0; i < 4; i++)
                    {
                        ipderecho[i] = mensaje.Buffer[17 + i];
                        portderecho += mensaje.Buffer[17 + 4 + i] << (i * 8);
                    }
                    VecinoSupIp = new IPEndPoint(new IPAddress(ipderecho), portderecho);
                    return response;
                case 20://Mensaje de respuesta de un nuevo nodo, de tipo 1
                    tempresponse = new byte[17];
                    tempresponse[0] = 2;
                    var VecinoIp = new IPEndPoint(IPAddress.Parse("127.0.0.1"), Chord.port);
                    byte[] key3 = new byte[16];
                    for(int i=0; i<16;i++)
                    {
                        key3[i] = mensaje.Buffer[i + 1];
                    }
                    int responsable = DeQuienEsLaTarea(key3);
                    if (responsable < 0)
                        VecinoIp = VecinoInfIp;
                    else if (responsable > 0)
                        VecinoIp = VecinoSupIp;
                    for (int i = 0; i < 16; i++)
                    {
                        if(mensaje.Buffer[i+1]!=ShaNuevo[i])
                        {
                            return response;
                        }
                    }
                    byte[] idecito = new byte[16];
                    for (int i=0;i<16;i++)
                    {
                        idecito[i] = ShaNuevo[i];
                        tempresponse[i + 1] = mensaje.Buffer[i + 1];
                    }
                    IDaNuevo[idecito] = mensaje.RemoteEndPoint;
                    response.Add(tempresponse);
                    destino.Add(VecinoIp);
                    return response;
                case 21://Instruccion 4 de respuesta a un WHOIS concerniente a un nuevo nodo
                    //for(int i=0; i<16;i++)
                    //{
                    //    if (mensaje.Buffer[1 + i] != mensaje.Buffer[17 + 4 + i])
                    //    {
                    //        goto caso4;
                    //    }
                    //}
                    tempresponse = new byte[17 + 8];
                    byte[] tempadr = direccionNuevo.Address.GetAddressBytes();
                    for (int e = 0; e < 4; e++)
                    {
                        tempresponse[e + 17] = tempadr[e];
                        tempresponse[e + 21] = (byte)(direccionNuevo.Port >> (8 * e));
                    }
                    for (int e = 0; e < 16; e++)
                    {
                        tempresponse[e + 1] = mensaje.Buffer[e + 1];
                    }
                    tempresponse[0] = 5;
                    response.Add(tempresponse);
                    destino.Add(mensaje.RemoteEndPoint);
                    goto caso4;

                default:
                    return response;
            }
        }

        public static bool EnRango(byte[] elemento, byte[] topeIzquierdo, byte[] topeDerecho)
        {
            return (comp.Compare(Resta(elemento, VecinoInf), Resta(ID, VecinoInf)) <= 0);
        }
        public static int DeQuienEsLaTarea(byte[] key2)
        {
            if (comp.Compare(VecinoInf, ID) == 0)
                return 0;
            if (EnRango(key2,VecinoInf,ID))
                return 0;
            else
            {
                byte[] restaResult = Resta(ID, key2);
                if (restaResult[0] >= 1 << 7)
                    return -1;
                return 1;
            }
        }

        static async Task Main(string[] args)
        {
            byte[] first = new byte[16];
            byte[] second = new byte[16];
            for(int i=0; i<16;i++)
            {
                first[i] = 128;
                second[i] = 64;
;            }
            byte[] result = Suma(second, Divide(Resta(first, second), 2));
            for(int i=0; i<16; i++)
            {
                Console.Write(result[i] + ",");
            }
            Console.WriteLine();
            ChordStart();
        }

        public static void ChordSystem()
        {
            Console.WriteLine("Escoja un puerto UDP");
            port = int.Parse(Console.ReadLine());
            udp = new UdpClient(port);
            Console.WriteLine("Desea iniciar un nuevo sistema(0) o unirse(1)");
            string inicio = (Console.ReadLine());
            if (inicio == "0")
            {
                SoyElPrimero = true;
                ID = new byte[16];
                random.NextBytes(ID);
                VecinoSup = ID.Clone() as byte[];
                VecinoInf = ID.Clone() as byte[];
                VecinoSupIp = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                VecinoInfIp = VecinoSupIp;
            }
            else
            {
                Console.WriteLine("Escriba el IP del nodo conocido del sistema");
                var ipAcceso = IPAddress.Parse(Console.ReadLine());
                Console.WriteLine("Escriba el puerto de acceso");
                int puertoAcceso = int.Parse(Console.ReadLine());
                byte[] peticionInicio = new byte[1];
                peticionInicio[0] = 6;
                Stopwatch stopwatch = new Stopwatch();
                var envio = udp.Send(peticionInicio, 1, new IPEndPoint(ipAcceso, puertoAcceso));
            receiveStart:
                var recibir = udp.BeginReceive(null, null);
                var endPointAcceso = new IPEndPoint(ipAcceso, puertoAcceso);
                var t = recibir.AsyncWaitHandle;
                while (true)
                {
                    Thread.Sleep(300);
                    if (recibir.IsCompleted)
                    {
                        break;
                    }
                    else
                    {
                        udp.BeginSend(peticionInicio, 1, new IPEndPoint(ipAcceso, puertoAcceso), null, null);
                    }
                }
                IPEndPoint remitente = null;
                var temp=udp.EndReceive(recibir, ref remitente);
                List<IPEndPoint> destinos = null;
                List<byte[]> IDs = null;
                UdpReceiveResult mensaje = new UdpReceiveResult(temp, remitente);
                if (temp[0]==9)
                {
                    goto receiveStart;
                }
                else if(temp[0]==0)
                {
                    var response=ProcesaMensajeConID(mensaje,out destinos,out IDs);
                    for(int i=0; i<response.Count;i++)
                    {
                        udp.BeginSend(response[i], response[i].Length,endPointAcceso, null, null);
                    }
                    goto receiveStart;
                }
                else if(temp[0]==8)
                {
                    var response = ProcesaMensajeConID(mensaje, out destinos, out IDs);
                }
            }
            
        }
        public static ConcurrentDictionary<byte[],IPEndPoint> DiccioDeIDaIP = new ConcurrentDictionary<byte[], IPEndPoint>(comp);
        public static ConcurrentDictionary<Paquete, bool> DiccioDePaquetes = new ConcurrentDictionary<Paquete, bool>(comp);
        public static ConcurrentQueue<Paquete> ColaAEnviar = new ConcurrentQueue<Paquete>();
        public static int eliminadosHandler = 0;
        public static void MainNetworkHandler(object parametros)
        {
            var Parametros = parametros as List<object>;
            int IntentosMaximos = (int)Parametros[0];
            long TiempoEsperaMaximo = (long)Parametros[1];
            ConcurrentQueue<Paquete> ColaYaEnviado = new ConcurrentQueue<Paquete>();
            Paquete sacado = null;
            while(true)
            {
                if(ColaAEnviar.TryDequeue(out sacado))
                {
                    if(sacado.tipo!=9)
                        DiccioDePaquetes[sacado] = true;
                }
                else if(!ColaYaEnviado.TryPeek(out sacado))
                {
                    Thread.Sleep(2);
                    continue;
                }
                else if(sacado.Tries>IntentosMaximos)
                {
                    bool dummy;
                    DiccioDePaquetes.TryRemove(sacado, out dummy);
                    if (dummy)
                        eliminadosHandler++;
                    continue;
                }
                else if(sacado.stopwatch.ElapsedMilliseconds<TiempoEsperaMaximo)
                {
                    continue;
                }
                else if(sacado.stopwatch.ElapsedMilliseconds >= TiempoEsperaMaximo)
                {
                    ColaYaEnviado.TryDequeue(out sacado);
                }
                sacado.stopwatch.Restart();
                sacado.Tries++;
                udp.Send(sacado.Mensaje, sacado.Mensaje.Length, sacado.DestinouOrigen);
                if(sacado.tipo!=9)
                   ColaYaEnviado.Enqueue(sacado);
            }
        }
        public static void AdicionaPaquete(UdpReceiveResult mensaje)
        {
            AdicionaPaquete(mensaje.Buffer, mensaje.RemoteEndPoint);
        }
        public static void AdicionaPaquete(byte[] mensaje, IPEndPoint destino)
        {
            Paquete adicionado = new Paquete(mensaje, destino);
            ColaAEnviar.Enqueue(adicionado);
        }
        public static IPEndPoint WHOISTHIS(byte[] id)
        {
            byte[] mensaje = new byte[25];
            mensaje[0] = 2;
            for(int i=0; i<16; i++)
            {
                mensaje[i + 9] = id[i];
            }
            Monitor.Enter(CountLock);
            for (int i = 0; i < 8; i++)
            {
                mensaje[i + 1] = (byte)(MessageID >> (i * 8));
            }
            MessageID++;
            Monitor.Exit(CountLock);
            IPEndPoint Elegido;
            int quien = DeQuienEsLaTarea(id);
            if (quien == 0)
                Elegido = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
            else if (quien < 0)
                Elegido = VecinoInfIp;
            else
                Elegido = VecinoSupIp;
            Paquete AEnviar = new Paquete(mensaje, Elegido);
            ColaAEnviar.Enqueue(AEnviar);
            while(true)
            {
                IPEndPoint result;
                if(DiccioDeIDaIP.TryRemove(id, out result))
                {
                    return result;
                }
                else
                {
                    Thread.Sleep(5);
                }
            }
        }
        public static List<IPEndPoint> WHOISTHIS(string nombre)
        {
            byte[] pedido=Encoding.Unicode.GetBytes(nombre);
            pedido=Hasher.ComputeHash(pedido);
            start:
            List<Thread> threads = new List<Thread>();
            ConcurrentQueue<IPEndPoint> lista = new ConcurrentQueue<IPEndPoint>();
            List<byte[]> ListaDeRequests = new List<byte[]>();

            for (int i = 0; i < 1 << ReplicationLevel; i++)
            {
                byte[] request = new byte[16];
                for (int e = 0; e < 16; e++)
                    request[e] = pedido[e];
                request[0] = (byte)(pedido[0] + (i << (7 - ReplicationLevel)));
                ListaDeRequests.Add(request);
                List<object> parametros = new List<object>();
                parametros.Add(lista);
                parametros.Add(request);
                Thread worker = new Thread(new ParameterizedThreadStart(WhoHandler));
                worker.Start(parametros);
                threads.Add(worker);
            }
                foreach (var t in threads)
                    t.Join(500);
            if (lista.Count < (1 << ReplicationLevel))
                goto start;
            var resp1 = lista.ToList();
            //resp1.RemoveAll(i => comp.Equals(i, new IPEndPoint(IPAddress.Parse("127.0.0.1"), Chord.port)));
            return resp1;
        }
        static void WhoHandler(object parametro)
        {
            var listaDeParams = (parametro as List<object>);
            var cola = listaDeParams[0] as ConcurrentQueue<IPEndPoint>;
            byte[] pedido = listaDeParams[1] as byte[];
            cola.Enqueue(WHOISTHIS(pedido));
        }
        public static void Verificador(object tiempodeespera)
        {
            int TiempodeEspera = (int)tiempodeespera;
            while(true)
            {
                byte[] peticion1 = new byte[48+9];
                byte[] peticion2 = new byte[48 + 9];
                random.NextBytes(peticion1);
                random.NextBytes(peticion2);
                peticion1[0] = 0;
                peticion2[0] = 0;
                Monitor.Enter(CountLock);
                for (int i = 0; i < 8; i++)
                {
                    peticion1[i + 1] = (byte)(MessageID >> (i * 8));
                }
                MessageID++;
                for (int i = 0; i < 8; i++)
                {
                    peticion2[i + 1] = (byte)(MessageID >> (i * 8));
                }
                MessageID++;
                Monitor.Exit(CountLock);
                ColaAEnviar.Enqueue(new Paquete(peticion1, VecinoInfIp));
                ColaAEnviar.Enqueue(new Paquete(peticion2, VecinoSupIp));
                Thread.Sleep(TiempodeEspera);
            }
        }


        public static void Receptor()
        {
            HashSet<Paquete> YaRecibidos = new HashSet<Paquete>(comp);
            Queue<Paquete> ColaYaRecibidos = new Queue<Paquete>();
            HashSet<IPEndPoint> Listade6 = new HashSet<IPEndPoint>(comp);
            //var receiver = udp.BeginReceive(null, null);
            int fivecount = 0;
            while (true)
            {
                IPEndPoint origen=null;

                byte[] mensaje=udp.Receive(ref origen);
                //receiver = udp.BeginReceive(null, null);
                //if (comp.Equals(origen, new IPEndPoint(IPAddress.Parse("192.168.1.102"), 7001))&&mensaje[0]==1)
                //    ;
                if (mensaje[0] == 4)
                    ;
                if (mensaje[0] == 2)
                    ;
                if (mensaje[0] == 5)
                    ;

                if (origen == null)
                    continue;
                Paquete LePaquet = new Paquete(mensaje, origen);
                if(YaRecibidos.Contains(LePaquet))
                {
                    continue;
                }
                else
                {
                    if (LePaquet.tipo == 6)
                        Listade6.Add(origen);
                    YaRecibidos.Add(LePaquet);
                    ColaYaRecibidos.Enqueue(LePaquet);
                    if(ColaYaRecibidos.Count>5000)
                    {
                        var temp = ColaYaRecibidos.Dequeue();
                        YaRecibidos.Remove(temp);
                    }
                }
                UdpReceiveResult recibido = new UdpReceiveResult(mensaje, origen);
                Paquete paquete = new Paquete(recibido);
                bool dummy;
                DiccioDePaquetes.TryRemove(paquete,out dummy);

                List<IPEndPoint> destino;
                List<byte[]> IDs;
                if (recibido.Buffer[0] == 4)
                    ;
                var temp2 = new UdpReceiveResult(recibido.Buffer.Clone() as byte[], recibido.RemoteEndPoint);
                var response=ProcesaMensajeConID(temp2, out destino, out IDs);
                
                

                for(int i=0; i<response.Count;i++)
                {
                    if (response[i][0] == 5 &&!comp.Equals(destino[i], new IPEndPoint(IPAddress.Parse("127.0.0.1"), Chord.port)))
                        ;
                    ColaAEnviar.Enqueue(new Paquete(response[i], destino[i]));
                }
            }
        }

        public static void ChordStart()
        {
            ChordSystem();
            List<object> Parametro = new List<object>(2);
            //Console.WriteLine("Escribe el máximo numero de intentos");
            int Tries = 3;
            Parametro.Add(Tries);
            //Console.WriteLine("Escribe el tiempo a esperar antes de otro reintento para los mensajes normales en ms");
            long tiempoMaximo = 100;
            Parametro.Add(tiempoMaximo);
            //Console.WriteLine("Escribe cada cuanto tiempo se intenta verificar el estado de los vecinos en ms");
            int tiempoMS = 500;
            Thread system = new Thread(new ParameterizedThreadStart(MainNetworkHandler));
            Thread receiver = new Thread(Receptor);
            //Thread verifier = new Thread(new ParameterizedThreadStart(Verificador));
            system.Name = "MainNetworkHandler";
            receiver.Name = "Receptor";
            //verifier.Name = "Verificador";
            system.Start(Parametro);
            receiver.Start();
            //verifier.Start(tiempoMS);
            //while(true)
            //{
            //    Thread.Sleep(2000);
            //    Console.WriteLine("Mi puerto es " + Chord.port);
            //    Console.WriteLine("Paquetes="+ColaAEnviar.Count);
            //    Console.WriteLine("Eliminados por network=" + eliminadosHandler);
            //    Console.WriteLine("He recibido esta cantidad de tipo 5=" + Tipo5);
            //}
        }
    }
}