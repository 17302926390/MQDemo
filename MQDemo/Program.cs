using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MQDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 1; i < 100; i++)
            {
                bool isTrue = SendMsg("first RabbitMQ message"+i, "firstQueue");
                string msg = isTrue ? "发送成功" : "发送失败";
                Console.WriteLine(msg+i);
                Thread.Sleep(1000);
            }
            Console.ReadKey();
            #region 原来的

            //var factory = new ConnectionFactory();
            //factory.HostName = "localhost";//主机名，Rabbit会拿这个IP生成一个endpoint，这个很熟悉吧，就是socket绑定的那个终结点。
            //factory.UserName = "guest";//默认用户名,用户可以在服务端自定义创建，有相关命令行
            //factory.Password = "guest";//默认密码

            //using (var connection = factory.CreateConnection())//连接服务器，即正在创建终结点。
            //{
            //    //创建一个通道，这个就是Rabbit自己定义的规则了，如果自己写消息队列，这个就可以开脑洞设计了
            //    //这里Rabbit的玩法就是一个通道channel下包含多个队列Queue
            //    using (var channel = connection.CreateModel())
            //    {
            //        //channel.QueueDeclare("kibaQueue", false, false, false, null);//创建一个名称为kibaqueue的消息队列
            //        //var properties = channel.CreateBasicProperties();
            //        //properties.DeliveryMode = 1;
            //        //string message = "I am Kiba5"; //传递的消息内容
            //        //byte[] buffer = Encoding.UTF8.GetBytes(message);
            //        //channel.BasicPublish("", "kibaQueue", properties, buffer); //生产消息
            //        //Console.WriteLine($"Send:{message}");

            //        for (int i = 0; i < 100; i++)
            //        {
            //            channel.QueueDeclare("kibaQueue", false, false, false, null);//创建一个名称为kibaQueue的消息队列
            //            var properties = channel.CreateBasicProperties();
            //            properties.DeliveryMode = 1;
            //            string message = "I am Kiba518"; //传递的消息内容
            //            channel.BasicPublish("", "kibaQueue", properties, Encoding.UTF8.GetBytes(message)); //生产消息
            //            Console.WriteLine($"Send:{message}");
            //            Thread.Sleep(1000);
            //        }
            //    }
            //}
            #endregion


        }


        /// <summary>
        /// RabbitMQ发送消息
        /// </summary>
        /// <param name="jsonstr">具体json格式的字符串</param>
        /// <param name="queuqname">具体入队的队列名称</param>
        /// <returns></returns>
        public static bool SendMsg(string jsonstr, string queuqname)
        {
            try
            {
                //1.实例化连接工厂
                var factory = new ConnectionFactory();
                 factory.HostName = "localhost";
            factory.UserName = "guest";
            factory.Password = "guest";
                factory.AutomaticRecoveryEnabled = true;////设置端口后自动恢复连接属性
                //2. 建立连接
                var connection = factory.CreateConnection();
                //3. 创建信道
                var channel = connection.CreateModel();
                try
                {
                    var queue_name = queuqname;//具体入队的队列名称
                    bool durable = true;//队列是否持久化
                    bool exclusive = false;
                    //设置 autoDeleted=true 的队列，当没有消费者之后，队列会自动被删除
                    bool autoDelete = false;
                    //4. 申明队列
                    channel.QueueDeclare(queue_name, durable, exclusive, autoDelete, null);

                    //将消息标记为持久性 - 将IBasicProperties.SetPersistent设置为true
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //持久化的消息

                    string message = jsonstr; //传递的消息内容
                    var body = Encoding.UTF8.GetBytes(message);

                    var exchange_name = "";
                    var routingKey = queue_name;//routingKey=queue_name，则为对应队列接收=queue_name

                    channel.BasicPublish(exchange_name, routingKey, properties, body); //开始传递(指定basicProperties) 

                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("RabbitMQ 发送数据异常：" + ex.Message);
                    //PubTool.ConnError("RabbitMQ", "RunLog", "发送数据异常：" + ex.Message);
                }
                finally
                {
                    connection.Close();
                    channel.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("RabbitMQ 外层调用发送方法，发生异常：" + ex.Message);
                //PubTool.ConnError("RabbitMQ", "RunLog", "外层调用发送方法，发生异常：" + ex.Message);
            }
            return false;
        }
    }
}
