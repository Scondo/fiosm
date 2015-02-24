# -*- coding: UTF-8 -*-
'''
Created on 11 нояб. 2013 г.

@author: Scondo
'''
import logging
from itertools import chain
try:
    import streetmangler
    locale = streetmangler.Locale('ru_RU')
    db = streetmangler.Database(locale)
    db.Load("data/ru_RU.txt")
    logging.info("Mangle OK")
except BaseException as e:
    db = None
    logging.warn(e.message)
    logging.warn("Mangle Broken")

synonyms = ((u'Первой Маёвки', u'Первой Маевки', u'1 Маевки'),
            (u'Нижняя Сыромятническая', u'Сыромятническая Ниж.', u'Ниж. Сыромятническая'),
            (u'Новая Сыромятническая', u'Сыромятническая Нов.', u'Нов. Сыромятническая'),
            (u'Нижний Сусальный', u'Сусальный Ниж.', u'Ниж. Сусальный'),
            (u'Верхний Сусальный', u'Сусальный Верхн.', u'Верхн. Сусальный'),
            (u'Нижний Таганский', u'Ниж. Таганский', u'Таганский Ниж.'),
            (u'Старый Толмачёвский', u'Толмачевский Ст.', u'Ст. Толмачевский'),
            (u'Пруд-Ключики', u'Пруд Ключики'),)
all_synonyms = set(chain(*synonyms))
from fiosm import numerals
numerals.generate_adj()


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


def nice(basename, shortname, fullname, place=False):
    basename = unslash(basename)
    # Few predefined states (supp. area)
    if fullname == u'город':
        yield u" ".join((fullname, basename))
    elif fullname == u'область':
        yield u" ".join((basename, fullname))
    elif fullname == u'край':
        yield u" ".join((basename, fullname))
    elif fullname == u'чувашия':  # Stupid, but real
        yield u" ".join((basename, u'Чувашия'))

    for name in check_synonym(basename):
        # Check when state part already in name
        if name.endswith((fullname, shortname + ".")) or\
          name.startswith((fullname, shortname + ".")):
            basename_ = name
        else:
            words = name.split(u' ')
            if words[-1] in numerals.adj:
                yield u' '.join([words[-1],] + [fullname,] + words[:-1])
                yield u' '.join([numerals.adj[words[-1]],] + [fullname,] + words[:-1])
            basename_ = u" ".join((fullname, name))

        if db is not None and not place:
            ma = db.CheckCanonicalForm(basename_)
            if ma:
                basename_ = ma[0]
        yield basename_
