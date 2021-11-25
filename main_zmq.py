import os
import time
from tcoreapi_mq import *
import threading
import queue

g_TradeZMQ = None
g_QuoteZMQ = None
g_TradeSession = ""
g_QuoteSession = ""
ReportID = ""
RealTimePrice = 0
trigger_queue = queue.Queue()


# 實時行情回補
def OnRealTimeQuote(symbol):
    print("實時行情", symbol["TradingPrice"])
    global RealTimePrice
    RealTimePrice = int(symbol["TradingPrice"])


# 已登入資金帳號變更
def OnGetAccount(account):
    print(account["BrokerID"])


# 實時委託回報消息
def OnexeReport(report):
    global ReportID
    print("OnexeReport:", report["ReportID"])
    ReportID = report["ReportID"]
    return None


# 實時成交回報回補
def RtnFillReport(report):
    print("RtnFillReport:", report["ReportID"])


# 查詢當日歷史委託回報回補
def ShowEXECUTIONREPORT(ZMQ, SessionKey, reportData):
    if reportData["Reply"] == "RESTOREREPORT":
        Orders = reportData["Orders"]
        if len(Orders) == 0:
            return
        last = ""
        for data in Orders:
            last = data
            print("查詢回報", data)
        reportData = g_TradeZMQ.QryReport(SessionKey, last["QryIndex"])
        ShowEXECUTIONREPORT(g_TradeZMQ, SessionKey, reportData)


# 查詢當日歷史委託成交回補
def ShowFillReport(ZMQ, SessionKey, reportData):
    if reportData["Reply"] == "RESTOREFILLREPORT":
        Orders = reportData["Orders"]
        if len(Orders) == 0:
            return

        last = ""
        for data in Orders:
            last = data
            print("查詢成交回報", data)
        reportData = g_TradeZMQ.QryFillReport(SessionKey, last["QryIndex"])
        ShowFillReport(g_TradeZMQ, SessionKey, reportData)


# 查詢部位消息回補
def ShowPOSITIONS(ZMQ, SessionKey, AccountMask, positionData):
    if positionData["Reply"] == "POSITIONS":
        position = positionData["Positions"]
        if len(position) == 0:
            return

        last = ""
        for data in position:
            last = data
            # print("部位:" + data["Symbol"])
            if "MXF" in data["Symbol"]:
                MXF_open = data["LongOpenPrice"]
                LongQty = data["SumLongQty"]

        positionData = g_TradeZMQ.QryPosition(SessionKey, AccountMask, last["QryIndex"])
        ShowPOSITIONS(g_TradeZMQ, SessionKey, AccountMask, positionData)
    return MXF_open, LongQty


# 交易消息接收
def trade_sub_th(obj, sub_port, filter=""):
    socket_sub = obj.context.socket(zmq.SUB)
    # socket_sub.RCVTIMEO=5000           #ZMQ超時設定
    socket_sub.connect("tcp://127.0.0.1:%s" % sub_port)
    socket_sub.setsockopt_string(zmq.SUBSCRIBE, filter)
    while True:
        message = socket_sub.recv()
        if message:
            message = json.loads(message[:-1])
            # print("in trade message",message)
            if message["DataType"] == "ACCOUNTS":
                for i in message["Accounts"]:
                    OnGetAccount(i)
            elif message["DataType"] == "EXECUTIONREPORT":
                OnexeReport(message["Report"])
            elif message["DataType"] == "FILLEDREPORT":
                RtnFillReport(message["Report"])


# 行情消息接收
def quote_sub_th(obj, sub_port, filter=""):
    socket_sub = obj.context.socket(zmq.SUB)
    # socket_sub.RCVTIMEO=7000   #ZMQ超時設定
    socket_sub.connect("tcp://127.0.0.1:%s" % sub_port)
    socket_sub.setsockopt_string(zmq.SUBSCRIBE, filter)
    while True:
        message = (socket_sub.recv()[:-1]).decode("utf-8")
        index = re.search(":", message).span()[1]  # filter
        message = message[index:]
        message = json.loads(message)
        # for message in messages:
        if message["DataType"] == "REALTIME":
            OnRealTimeQuote(message["Quote"])
        elif message["DataType"] == "TICKS" or message["DataType"] == "1K" or message["DataType"] == "DK":
            # print("@@@@@@@@@@@@@@@@@@@@@@@",message)
            strQryIndex = ""
            while True:
                s_history = obj.GetHistory(g_QuoteSession, message["Symbol"], message["DataType"], message["StartTime"],
                                           message["EndTime"], strQryIndex)
                historyData = s_history["HisData"]
                if len(historyData) == 0:
                    break

                last = ""
                for data in historyData:
                    last = data
                    print("歷史行情：Time:%s, Volume:%s, QryIndex:%s" % (data["Time"], data["Volume"], data["QryIndex"]))

                strQryIndex = last["QryIndex"]

    return


def searchQty(arrInfo):
    strAccountMask = arrInfo[0]["AccountMask"]
    # 查詢持倉
    positionData = g_TradeZMQ.QryPosition(g_TradeSession, strAccountMask, "")
    # print('持倉查詢:', positionData)
    OpenPrice = ShowPOSITIONS(g_TradeZMQ, g_TradeSession, strAccountMask, positionData)[0]
    Quantity = ShowPOSITIONS(g_TradeZMQ, g_TradeSession, strAccountMask, positionData)[1]

    return OpenPrice, int(Quantity)


# 交易策略邏輯判斷
def strategy(overweight_list, overweight_pz):
    while True:
        global RealTimePrice
        global trigger_queue
        if RealTimePrice >= overweight_list[overweight_pz] and trigger_queue.qsize() == 0:  # 依照目前部位大小從理論價陣列[index]判斷
            print("加碼囉!! @%d" % (overweight_list[overweight_pz]))
            trigger_queue.put(1)
            overweight_pz += 1

        time.sleep(0.001)


def main():
    global g_TradeZMQ
    global g_QuoteZMQ
    global g_TradeSession
    global g_QuoteSession
    global trigger_queue

    # -----------------connect zmq----------------- #
    g_TradeZMQ = TradeAPI("ZMQ", "8076c9867a372d2a9a814ae710c256e2")
    g_QuoteZMQ = QuoteAPI("ZMQ", "8076c9867a372d2a9a814ae710c256e2")

    t_data = g_TradeZMQ.Connect("51141")
    q_data = g_QuoteZMQ.Connect("51171")
    print("t_data=", t_data)
    print("q_data=", q_data)

    if q_data["Success"] != "OK":
        print("[quote]connection failed")
        return

    if t_data["Success"] != "OK":
        print("[trade]connection failed")
        return

    g_TradeSession = t_data["SessionKey"]
    g_QuoteSession = q_data["SessionKey"]

    # -----------------行情----------------- #
    # 建立一個行情線程
    quoteSymbol = "TC.F.TWF.MXF.HOT"
    t2 = threading.Thread(target=quote_sub_th, args=(g_QuoteZMQ, q_data["SubPort"],))
    t2.start()
    # 解除訂閱(每次訂閱合約前，先解除，避免重複訂閱)
    g_QuoteZMQ.UnsubQuote(g_QuoteSession, quoteSymbol)
    # 訂閱實時行情
    g_QuoteZMQ.SubQuote(g_QuoteSession, quoteSymbol)

    # -----------------交易----------------- #
    # 建立一個交易線程
    t1 = threading.Thread(target=trade_sub_th, args=(g_TradeZMQ, t_data["SubPort"],))
    t1.start()

    # 查詢已登入資金帳號
    accountInfo = g_TradeZMQ.QryAccount(g_TradeSession)
    if accountInfo is not None:
        arrInfo = accountInfo["Accounts"]
        Price, overweight_pz = searchQty(arrInfo)

        # 如果部位是1時，尚未建立OpenPrice，就把紀錄新倉價格紀錄到txt檔
        filepath = 'OpenPrice.txt'
        if os.path.isfile(filepath) is False and overweight_pz == 1:
            f = open(filepath, 'w')
            f.write(Price)
            f.close()

        f = open(filepath, 'r')
        OpenPrice = int(f.read())
        f.close()

        # 每50點加碼一口  # 紀錄理論價格
        delta = 80
        overweight_list = [OpenPrice]
        for count in range(100):
            OpenPrice += delta
            count += 1
            overweight_list.append(OpenPrice)

        # 建立一個加碼偵測線程
        t3 = threading.Thread(target=strategy, args=(overweight_list, overweight_pz))
        t3.start()

        while True:
            if trigger_queue.qsize() != 0:
                print("觸發下單")

                # 下單
                orders_obj = {
                    "Symbol": "TC.F.TWF.MXF.HOT",
                    "BrokerID": arrInfo[0]['BrokerID'],
                    "Account": arrInfo[0]['Account'],
                    # "Price": "15000",
                    "TimeInForce": "3",
                    "Side": "1",
                    "OrderType": "1",
                    "OrderQty": "1",
                    "PositionEffect": "0"
                }
                s_order = g_TradeZMQ.NewOrder(g_TradeSession, orders_obj)
                print('下單結果:', s_order)
                if s_order['Success'] == "OK":
                    print("下單成功")
                    trigger_queue.get(1)

            time.sleep(0.001)


if __name__ == '__main__':
    main()
