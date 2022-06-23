from random import randint
from rand_engine.templates import template_streaming
from rand_engine.core_streaming import gen_str_num, gen_value, gen_money, gen_distinct, \
                gen_diff_day, gen_diff_random_day, gen_discrete
from datetime import datetime as dt

def get_timestamp_op():
    return dt.now().strftime("%Y-%m-%dT%H:%M:%S")


def gen_creditCard_operation(kwargs):
    return {
        "operacao": f"{gen_str_num(10)}-{gen_str_num(2)}",
        "cliente": gen_distinct(distinct=kwargs['clients']),
        "contraparte": gen_distinct(distinct=kwargs['clients']),
        "tipo": gen_distinct(["reembolso", "compra"]),
        "meio": gen_distinct(["online", "cartão fisico"]),
        "valor": gen_money(1, 1000, chance=0.35, factor=10),
        "timestamp_op" : get_timestamp_op()
    }


def gen_pix_operation(kwargs):
    return {
        "operacao": f"{gen_str_num(10)}-{gen_str_num(2)}",
        "cliente": gen_distinct(distinct=kwargs['clients']),
        "contraparte": gen_distinct(distinct=kwargs['clients']),
        "valor": gen_money(1, 10**5),
        "timestamp_op": get_timestamp_op(),
    }


def gen_bill_operation(kwargs):
    data_inicio = dt.now().strftime("%Y-%m-%d")
    data_vencimento = gen_diff_day(now=True, diff_day=5, formato="%Y-%m-%d")
    return {
        "operacao": f"{gen_str_num(10)}-{gen_str_num(2)}",
        "cliente": gen_distinct(distinct=kwargs['clients']),
        "contraparte": gen_distinct(distinct=kwargs['clients']),
        "valor": gen_money(1, 10**4),
        "data_emissao": data_inicio,
        "data_vencimento": data_vencimento,
        "timestamp_op" : get_timestamp_op()
    }


if __name__ == '__main__':
    pass
   
