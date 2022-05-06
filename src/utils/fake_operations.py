from random import randint
from rand_engine.templates import template_streaming
from rand_engine.core_streaming import gen_str_num, gen_value, gen_money, gen_distinct, \
                gen_diff_day, gen_diff_random_day, gen_discrete
from datetime import datetime as dt

tickers = ["CAB91", "GAA77", "PETR4"]

contrapartes = ["Jaú Serve", "Espetinho do Bicão", "Catarino", "Posto Esquinão", "Sapataria da cidade"]


def get_timestamp_op():
    return dt.now().strftime("%Y-%m-%dT%H:%M:%S")


def gen_investment_base(tipo):
    return {
        "operacao": f"{tipo}-{gen_str_num(10)}",
        "volume": gen_value(1, 1000, chance=0.2, factor=10),
        "risco": gen_value(0, 100),
        "valor": gen_money(1, 1000, chance=0.35, factor=10),
    }


def gen_stocks(kwargs):
    data_inicio = dt.now().strftime("%Y-%m-%d")
    return {
        "client_id": str(randint(0, kwargs.get("num_clients", 1000))),
        **gen_investment_base(kwargs.get("tipo", "STOCK")),
        "ticker": f"{gen_distinct(tickers)}",
        "data_inicio": data_inicio,
        "timestamp_op" : get_timestamp_op()
    }


def gen_investment(kwargs):
    data_inicio = dt.now().strftime("%Y-%m-%d")
    tipo = kwargs.get("tipo", "CDB")
    data_vencimento = gen_diff_day(now=True, diff_day=45, formato="%Y-%m-%d")
    return {
        "client_id": randint(0, kwargs.get("num_clients", 1000)),
        **gen_investment_base(tipo),
        "descricao": f"{tipo} Investment",
        "tipo": tipo,
        "profit": 0,
        "data_inicio": data_inicio,
        "data_vencimento": data_vencimento,
        "timestamp_op" : get_timestamp_op()
    }
    

def get_creditCard_operation(kwargs):
    return {
        "client_id": randint(0, kwargs.get("num_clients", 1000)),
        "tipo": gen_distinct(["reembolso", "compra"]),
        "meio": gen_distinct(["online", "cartão fisico"]),
        "contraparte": gen_distinct(contrapartes),
        "valor": gen_money(1, 1000, chance=0.35, factor=10),
        "timestamp_op" : get_timestamp_op()
    }


def get_pix_operation(kwargs):
    return {
        "client_id": randint(0, kwargs.get("num_clients", 1000)),
        "contraparte": randint(0, kwargs.get("num_clients", 1000)),
        "valor": gen_money(1, 10**5),
        "timestamp_op": get_timestamp_op(),
    }


def get_bill_operation(kwargs):
    data_inicio = dt.now().strftime("%Y-%m-%d")
    data_vencimento = gen_diff_day(now=True, diff_day=5, formato="%Y-%m-%d")
    return {
        "operacao": f"{gen_str_num(10)}-{gen_str_num(2)}",
        "client_id": randint(0, kwargs.get("num_clients", 1000)),
        "contraparte": randint(0, kwargs.get("num_clients", 1000)),
        "valor": gen_money(1, 10**4),
        "data_emissao": data_inicio,
        "data_vencimento": data_vencimento,
        "timestamp_op" : get_timestamp_op()
    }


if __name__ == '__main__':

    print("\n\nINVESTMENTS TIPO CDB")
    res3 = gen_investment(tipo="CDB")
    print(res3)
    
    print("\n\nINVESTMENTS TIPO LCA")
    res5 = gen_investment(tipo="LCA")
    print(res5)
    
    print("\n\nINVESTMENTS TIPO LCI")
    res7 = gen_investment(tipo="LCI")
    print(res7)
    
    print("\n\nINVESTMENTS TIPO CRI")
    res9 = gen_investment(tipo="CRI")
    print(res9)
    
    print("\n\nINVESTMENTS TIPO CRA")
    res11 = gen_investment(tipo="CRA")
    print(res11)
   
