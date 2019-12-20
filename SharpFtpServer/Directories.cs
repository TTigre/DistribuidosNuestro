using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using System.IO;

namespace SharpFtpServer
{
    [Serializable]
    public class MyDirectory
    {
        [XmlAttribute("name")]
        public string Name { get; set; }
        [XmlAttribute("realname")]
        public string Realname { get; set; }

        [XmlAttribute("date")]
        public DateTime Date { get; set; }
        [XmlAttribute("Size")]
        public long Size { get; set; }
        [XmlAttribute("isfile")]
        public bool IsFile { get; set; }

    }
    [Serializable]
    public class MyFileDescriptor
    {
        [XmlAttribute("name")]
        public string Name { get; set; }

        [XmlAttribute("date")]
        public DateTime Date { get; set; }

        [XmlAttribute("Size")]
        public long Size { get; set; }
        [XmlAttribute("Parts")]
        public long Parts { get; set; }
    }
   public static class DirectoryMethods
    {
        public static FileStream CreateDirectory(string pathname)
        {
            string[] a = pathname.Split('\\');
            if(a[a.Length-1]=="infod")
            {
                pathname = a[0];
                for (int i = 1; i < a.Length - 1; i++)
                    pathname += @"\"+a[i];
            }
            Directory.CreateDirectory(pathname);
            List<MyDirectory> directories = new List<MyDirectory>();
            XmlSerializer serializer = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            using (StreamWriter w = new StreamWriter(pathname + "\\infod")) 
            {
                serializer.Serialize(w, directories);
            }
            return new FileStream(pathname + "\\infod", FileMode.Open);
        }
        public static FileStream CreateDescriptor(string pathname, long size)
        {
            List<MyFileDescriptor> files = new List<MyFileDescriptor>();
            XmlSerializer serializer = new XmlSerializer(files.GetType(), new XmlRootAttribute("MyFileDescriptor"));
            using (StreamWriter w = new StreamWriter(pathname + ".info"))
            {
                files.Add(new MyFileDescriptor
                {
                    Name = pathname,
                    Date = System.DateTime.Now,
                    Size = size,
                    Parts = (size % (1 << 18) == 0) ? size / (1 << 18) : size / (1 << 18) + 1
                });
                serializer.Serialize(w, files);
            }
            return new FileStream(pathname + ".info", FileMode.Open);
        }
        public static FileStream AddAtDirectory(FileStream directory, string name,string realname, long size, bool isfile=false)
        {
            List<MyDirectory> directories = new List<MyDirectory>();
            XmlSerializer serializer = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            directories = serializer.Deserialize(new StreamReader(directory)) as List<MyDirectory>;
            directories.Add(new MyDirectory
            {
                Name = name,
                Realname = realname,
                Date = System.DateTime.Now,
                Size = size,
                IsFile = isfile
                });
            directory.Position = 0;
            directory.SetLength(0);
            directory.Flush();
            StreamWriter w = new StreamWriter(directory);
                serializer.Serialize(w, directories);
            directory.Position = 0;
            return directory;
        }
        public static FileStream ChangeAtDirectory(FileStream directory, string name, string realname, long size, bool isfile = false)
        {
            List<MyDirectory> directories = new List<MyDirectory>();
            XmlSerializer serializer = new XmlSerializer(directories.GetType(), new XmlRootAttribute("MyDirectory"));
            directories = serializer.Deserialize(new StreamReader(directory)) as List<MyDirectory>;
            foreach(var i in directories.Where(k => k.Name == name))
            {
                i.Realname = realname;
                i.Date = System.DateTime.Now;
                i.Size = size;
                i.IsFile = isfile;
            }
            directory.Position = 0;
            directory.SetLength(0);
            directory.Flush();
            StreamWriter w = new StreamWriter(directory);
            serializer.Serialize(w, directories);
            directory.Position = 0;
            return directory;
        }

    }
}
