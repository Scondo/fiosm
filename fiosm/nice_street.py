# -*- coding: UTF-8 -*-
'''
Created on 11 нояб. 2013 г.

@author: Scondo
'''
import logging
import pymorphy2
import pymorphy2.units
import numerals

from itertools import chain


synonyms = ((u'Первой Маёвки', u'Первой Маевки', u'1 Маевки'),
#            (u'Нижняя Сыромятническая', u'Сыромятническая Ниж.', u'Ниж. Сыромятническая'),
#            (u'Новая Сыромятническая', u'Сыромятническая Нов.', u'Нов. Сыромятническая'),
#            (u'Нижний Сусальный', u'Сусальный Ниж.', u'Ниж. Сусальный'),
#            (u'Верхний Сусальный', u'Сусальный Верхн.', u'Верхн. Сусальный'),
#            (u'Нижний Таганский', u'Ниж. Таганский', u'Таганский Ниж.'),
#            (u'Старый Толмачёвский', u'Толмачевский Ст.', u'Ст. Толмачевский'),
            (u'Пруд-Ключики', u'Пруд Ключики'),)
all_synonyms = set(chain(*synonyms))
descr = {u'Большой': (u'Бол.', u'Б.'),
         u'Верхний': (u'Верхн.', u'Верх.', u'В.'),
         u'Восточный': (u'Вост.', u'В.'),
         u'Западный': (u'Зап.', u'З.'),
         u'Левый': (u'Лев.', u'Л.'),
         u'Малый': (u'Мал.', u'М.'),
         u'Нижний': (u'Ниж.', u'Н.'),
         u'Новый': (u'Нов.', u'Н.'),
         u'Правый': (u'Пр.', u'П.'),
         u'Северный': (u'Сев.', u'С.'),
         u'Средний': (u'Ср.', u'С.'),
         u'Старый': (u'Стар.', u'С.'),
         u'Южный': (u'Юж.', u'Ю.')}


# prepare
numerals.generate_adj()
all_num = set(chain(*numerals.adj.items()))
morph = pymorphy2.MorphAnalyzer()
suff_morph = pymorphy2.units.KnownSuffixAnalyzer(morph)


def desc_all_g():
    for full, socr in descr.iteritems():
        yield full
        f_ = morph.parse(full)[0]
        yield f_.inflect({'femn'})
        yield f_.inflect({'neut'})
        for s in socr:
            yield s
desc_all = set(desc_all_g())


def get_descr(words, fullname):
    desc_word = u''
    if words[0] in desc_all:
        desc_word = words[0]
        words = words[1:]
    elif words[-1] in desc_all:
        desc_word = words[-1]
        words = words[:-1]
    if desc_word.endswith(u'.'):
        res = set()
        full_lex = morph.parse(fullname)[0]
        for full, socr in descr.iteritems():
            if desc_word in socr:
                res.update(set(socr))
                desc_lex = [it for it in morph.parse(full)
                            if {'ADJF'} in it.tag]
                res.add(desc_lex[0].inflect({full_lex.tag.gender}).word)
        # todo: перейти обратно к списку, чтобы полные названия были в начале
        return (res, words)
    else:
        # TODO: Добавить проверку/пропуск на рассогласование
        return ((desc_word,), words)


def get_num(words, fullname):
    num_word = u''
    if words[0] in numerals.adj:
        num_word = words[0]
        words = words[1:]
    elif words[-1] in numerals.adj:
        num_word = words[-1]
        words = words[:-1]
    if num_word:
        # TODO: Добавить проверку/пропуск на рассогласование
        return ((num_word, numerals.adj[num_word]), words)
    else:
        return ((num_word,), words)


def get_repr_tag(words):
    def impossible(tag):
        if {'Surn', 'nomn'} in tag:
            return True
        if {'Name', 'nomn'} in tag:
            return True
        return False
    repr_word = words[-1]
    #if repr_word.isdigit():
    #    repr_word = words[-2]
    lexemes = morph.parse(repr_word)
    for lex in lexemes:
        if not impossible(lex.tag):
            return lex.tag
    # Default parse contains only impossible lexemes
    lexemes = suff_morph.parse(repr_word, repr_word.lower(), set())
    for lex in lexemes:
        if not impossible(lex[1]):
            return lex[1]


def check_synonym(name):
    if name in all_synonyms:
        for group in synonyms:
            if name in group:
                return group
    else:
        return (name,)


def unslash(basename):
    '''Replace slashes with parentheses
    '''
    if basename.count('/') == 2:
        basename = basename.replace('/', '(', 1)
        basename = basename.replace('/', ')', 1)
    return basename


def btr_join(parts, space=u' '):
    parts = filter(None, parts)
    return space.join(parts)


def nice(basename, shortname, fullname, place=False):
    basename = unslash(basename)
    # Few predefined states (supp. area)
    # Если статусная часть представляет собой слова «город»,
    # «деревня», «отделение совхоза», «посёлок», «совхоз»
    # или «станция», то она пишется перед основной частью.
    if fullname in (u'город', u'деревня',
                    u'отделение совхоза', u'совхоз',
                    u'посёлок', u'поселок', u'станция'):
        yield u" ".join((fullname, basename))
    elif fullname == u'область':
        yield u" ".join((basename, fullname))
    elif fullname == u'край':
        yield u" ".join((basename, fullname))

    for name in check_synonym(basename):
        # Check when state part already in name
        if name.endswith((fullname, shortname + ".")) or\
          name.startswith((fullname, shortname + ".")):
            basename_ = name
        else:
            words = name.split(u' ')
            if len(words) > 1:
                nums, words = get_num(words, fullname)
            else:
                nums = ('',)
            if len(words) > 1:
                descs, words = get_descr(words, fullname)
            else:
                descs = ('',)

            repr_tag = get_repr_tag(words)
            if repr_tag is not None:
                for num in nums:
                    for desc in descs:
                        if {'ADJF'} in repr_tag:
                            yield btr_join([num, desc] + words + [fullname, ])
                            yield btr_join([num, ] + words + [desc, fullname, ])
                        else:
                            yield btr_join([desc, num, fullname] + words)
                            yield btr_join([num, desc, fullname] + words)
            basename_ = u" ".join((fullname, name))

        yield basename_
