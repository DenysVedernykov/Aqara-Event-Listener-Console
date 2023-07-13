using System.Net;
using Microsoft.VisualBasic;
using System.Text;
using NewLife.RocketMQ;
using Newtonsoft.Json;
using RocketMQ.NETClient.Consumer;
using RocketMQ.NETClient.Message;
using System.Net.Sockets;
using javax.xml.crypto;

const int PORT = 7834;

UdpClient udpClient = new();

ListenToAqara();

while (true)
{
    SendMessage(Encoding.UTF8.GetBytes("hello"));

    var tokenSource = new CancellationTokenSource();

    //tokenSource.CancelAfter(300);

    try
    {
        var receive = await udpClient.ReceiveAsync(tokenSource.Token);
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.Message);
        continue;
    }

    Thread.Sleep(1000);
}

const string mqAddress = "uspro-opdmq-broker1.aqara.com:9876";
const string appId = "sss";
const string keyId = "sss";
const string appKey = "sss";

void SendMessage(byte[] messageBytes)
{
    try
    {
        var broadcastIPAddress = IPAddress.Broadcast.ToString();

        udpClient.Send(messageBytes, broadcastIPAddress, PORT);

        Console.WriteLine(Encoding.UTF8.GetString(messageBytes));
    }
    catch (Exception ex)
    {
    }
}

void ListenToAqara()
{
    try
    {
        var consumer = new Consumer()
        {
            AclOptions = new AclOptions() { AccessKey = keyId, SecretKey = appKey },
            NameServerAddress = mqAddress,
            Group = appId,
            Topic = appId,
            FromLastOffset = false,
            BatchSize = 20,
        };

        consumer.Stop();

        consumer.Consumed += Consumer_Consumed;

        var a = consumer.Start();

        while (true)
        {
            Thread.Sleep(1000);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.Message);
    }
}

void Consumer_Consumed(object? sender, NewLife.RocketMQ.Models.ConsumeEventArgs e)
{
    Console.WriteLine($"\n-------------------");
    Console.WriteLine($"Queue:{e.Queue}");

    foreach (var msg in e.Messages)
    {
        Console.WriteLine($"Message {msg.MsgId}:{msg.BodyString}");
        Console.WriteLine($"BornHost: {msg.BornHost}");
        SendMessage(msg.Body);
    }

    Console.WriteLine($"Result:{e.Result.Status}");
    Console.WriteLine($"-------------------\n");
}

