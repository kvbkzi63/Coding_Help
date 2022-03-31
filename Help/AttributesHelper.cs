using System;

namespace Help
{
    public class AttributeHelper
    {
    }

    /// <summary>
    /// DB User 重新導向處
    /// </summary>
    public class DBUserAttribute : Attribute
    {
        public string Name
        {
            get; set;
        }

        public DBUserAttribute(string UserName)
        {
            Name = UserName;
        }

    }

    /// <summary>
    /// 不組進DB Insert Script中
    /// </summary>
    public class NoWrite : Attribute
    {

    }
}
