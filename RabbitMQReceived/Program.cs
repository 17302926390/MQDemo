using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQReceived
{
    class Program
    {
        static void Main(string[] args)
        {
            string queuqname = "firstQueue";
            ushort limitnum = 3;

            try
            {
                #region 构建消息队列
                //1.实例化连接工厂
                var factory = new RabbitMQ.Client.ConnectionFactory();
                factory.HostName = "localhost";
                factory.UserName = "guest";
                factory.Password = "guest";
                factory.AutomaticRecoveryEnabled = true;
                //2. 建立连接 
                var connection = factory.CreateConnection();
                //3. 创建信道
                var channel = connection.CreateModel();

                var queue_name = queuqname;//项目下游上传的队列信息
                bool durable = true;//队列是否持久化
                bool exclusive = false;
                //设置 autoDeleted=true 的队列，当没有消费者之后，队列会自动被删除
                bool autoDelete = false;
                //4. 申明队列
                channel.QueueDeclare(queue_name, durable, exclusive, autoDelete, null);
                //5. 构造消费者实例
                var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(channel);
                bool autoAck = false;
                //autoAck:true；自动进行消息确认，当消费端接收到消息后，就自动发送ack信号，不管消息是否正确处理完毕
                //autoAck:false；关闭自动消息确认，通过调用BasicAck方法手动进行消息确认 
                //6. 绑定消息接收后的事件委托

                //8. 启动消费者
                //设置prefetchCount : 3 来告知RabbitMQ，在未收到消费端的N条消息确认时，不再分发消息，也就确保了当消费端处于忙碌状态时
                channel.BasicQos(0, limitnum, false);

                channel.BasicConsume(queue_name, autoAck, consumer);

                #endregion

                #region 队列-接收消息的处理方法

                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        //var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        //获取消息后进行操作，do something
                        bool flag = false;
                        if (!string.IsNullOrEmpty(message))
                        {
                            try
                            {
                                //做其他存储或处理操作
                                //File.WriteAllText(@"C:\Users\Administrator\Desktop\333.txt", message, Encoding.UTF8);
                                Console.WriteLine("接收消息：" + message);
                                flag = true;


                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        else
                        {
                            flag = true;
                        }
                        if (flag)
                        {
                            //操作完毕，则手动确认消息可删除
                            // 7. 发送消息确认信号（手动消息确认）
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                };
                #endregion

            }
            catch (Exception ex)
            { 
            
            }
            //finally
            //{
            //    connection.Close();//不能关，关了就停止接收消息了
            //    channel.Close();
            //}
           

            Console.ReadKey();
        }

        static void Methed(object model, BasicDeliverEventArgs ea, IModel channel)
        {
            try
            {
                //var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                //获取消息后进行操作，do something
                bool flag = false;
                if (!string.IsNullOrEmpty(message))
                {
                    try
                    {
                        //做其他存储或处理操作
                        File.WriteAllText(@"C:\Users\Administrator\Desktop\333.txt", message, Encoding.UTF8);
                        Console.WriteLine("ok :" + message);
                        flag = true;


                    }
                    catch (Exception ex)
                    {
                    }
                }
                else
                {
                    flag = true;
                }
                if (flag)
                {
                    //操作完毕，则手动确认消息可删除
                    // 7. 发送消息确认信号（手动消息确认）
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
            }
            catch (Exception ex)
            {
            }
        }
       
    }

     
}
