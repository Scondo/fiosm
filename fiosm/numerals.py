# -*- coding: UTF-8 -*-
'''
Created on 19 дек. 2014 г.

@author: Scondo
http://ru.wikibooks.org/wiki/%D0%A0%D1%83%D1%81%D1%81%D0%BA%D0%B8%D0%B9_%D1%8F%D0%B7%D1%8B%D0%BA/%D0%9D%D0%B0%D1%80%D0%B0%D1%89%D0%B5%D0%BD%D0%B8%D0%B5_%D0%BE%D0%BA%D0%BE%D0%BD%D1%87%D0%B0%D0%BD%D0%B8%D0%B9_%D0%BA_%D1%87%D0%B8%D1%81%D0%BB%D0%B8%D1%82%D0%B5%D0%BB%D1%8C%D0%BD%D1%8B%D0%BC_%D0%B2_%D1%86%D0%B8%D1%84%D1%80%D0%BE%D0%B2%D0%BE%D0%B9_%D1%84%D0%BE%D1%80%D0%BC%D0%B5
'''
import pymorphy2
base_adj = {1: u"первый",
            2: u"второй",
            3: u"третий",
            4: u"четвёртый",
            5: u"пятый",
            6: u"шестой",
            7: u"седьмой",
            8: u"восьмой",
            9: u"девятый",}


adj = {}


def generate_adj(genders=True):
    global adj
    morph = pymorphy2.MorphAnalyzer()
    if genders is True:
        genders = ({'masc'}, {'femn'}, {'neut'})
    if genders:
        matrix = genders
    else:
        matrix = ({},)

    if matrix:
        for n, wordbase in base_adj.viewitems():
            lex = morph.parse(wordbase)[0]
            for it in matrix:
                word = lex.inflect(it).word
                # Падежное окончание должно быть:
                # 1) ОДНОБУКВЕННЫМ, если ПРЕДПОСЛЕДНЯЯ буква числительного
                #  ГЛАСНАЯ или МЯГКИЙ ЗНАК: десятЫй, десятОй — 10-й; десятОе,
                #  десятЫе — 10-е; десятЫм, десятОм — 10-м: десятАя — 10-я;
                #  десятЫх — 10-х; третЬя — 3-я;
                # 2) ДВУХБУКВЕННЫМ, если ПРЕДПОСЛЕДНЯЯ буква СОГЛАСНАЯ:
                #  десятоГо — 10-го; десятоМу — 10-му; десятыМи — 10-ми.
                if word[-2] in u'ёуеыаоэяиюь':
                    key = str(n) + '-' + word[-1]
                else:
                    key = str(n) + '-' + word[-2:]
                adj[key] = word

if __name__ == "__main__":
    generate_adj()
    for n, word in adj.viewitems():
        print word, n

    pass