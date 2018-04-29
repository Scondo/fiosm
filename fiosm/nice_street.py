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


synonyms = ((u'Первой Маёвки', u'1 Маевки'),
            (u'Пруд-Ключики', u'Пруд Ключики'),
            (u'Льва Толстого', u'Л.Толстого', u'Л. Толстого', u'Толстого'),
            (u'Максима Горького', u'М.Горького', u'М. Горького', u'Горького'),
            (u'Константина Воробьёва', u'Константина Воробьева',
             u'К.Воробьёва', u'К.Воробьева', u'Воробьёва', u'Воробьева'),
            (u'Константина Царёва', u'Константина Царева',
             u'К.Царёва', u'К.Царева', u'Царёва', u'Царева'),
            (u'Академика Королёва', u'Академика Королева',
             u'Королёва', u'Королева'),
            (u'лейтенанта Шмидта', u'л-та Шмидта', u'Шмидта'),
            (u'гаражно-строительный кооператив', u'гаражно-строительный кооп.')
            )
jo = {u'Артём': u'Артем',
      u'Берёзов': u'Березов',
      u'Жигулёв': u'Жигулев',
      u'Звёздн': u'Звездн',
      u'Зелён': u'Зелен',
      u'Казён': u'Казен',
      u'Каланчёв': u'Каланчев',
      u'Королёв': u'Королев',
      u'Краснознамён': u'Краснознамен',
      u'Кремлёв': u'Кремлев',
      u'Лётн': u'Летн',
      u'Лётч': u'Летч',
      u'Молодёжн': u'Молодежн',
      u'Новосёлов': u'Новоселов',
      u'Озёрн': u'Озерн',
      u'Пугачёв': u'Пугачев',
      u'Толмачёв': u'Толмачев',
      u'Трёх': u'Трех',
      u'Семёнов': u'Семенов',
      u'Филёв': u'Филев',
      u'Хрущёв': u'Хрущев',
      }

all_synonyms = set(chain(*synonyms))
descr = {u'Большой': (u'Бол.', u'Б.'),
         u'Верхний': (u'Верхн.', u'Верх.', u'В.'),
         u'Восточный': (u'Вост.', u'В.'),
         u'Западный': (u'Зап.', u'З.'),
         u'Левый': (u'Лев.', u'Л.'),
         u'Малый': (u'Мал.', u'М.'),
         u'Нижний': (u'Нижн.', u'Ниж.', u'Н.'),
         u'Новый': (u'Нов.', u'Н.'),
         u'Правый': (u'Пр.', u'П.'),
         u'Северный': (u'Сев.', u'С.'),
         u'Средний': (u'Ср.', u'С.'),
         u'Старый': (u'Стар.', u'Ст.', u'С.'),
         u'Южный': (u'Юж.', u'Ю.')}


# prepare
numerals.generate_adj()
all_num = set(chain(*numerals.adj.items()))
morph = pymorphy2.MorphAnalyzer()
suff_morph = pymorphy2.units.KnownSuffixAnalyzer(morph)


def desc_all_g():
    for full, socr in descr.items():
        yield full
        f_ = [it for it in morph.parse(full)
              if {'ADJF', 'nomn', 'masc'} in it.tag][0]
        fem = f_.inflect({'femn'})
        if fem:
            yield fem.word
        neut = f_.inflect({'neut'})
        if neut:
            yield neut.word
        for s in socr:
            yield s
desc_all = set(desc_all_g())


def get_descr(words, fullname):
    """Разделение слова на описательное и остальные"""
    desc_word = u''
    if words[0] in desc_all:
        desc_word = words[0]
        words = words[1:]
    elif words[-1] in desc_all:
        desc_word = words[-1]
        words = words[:-1]
    if desc_word.endswith(u'.'):
        res = []
        # небольшой набор костылей
        if fullname.endswith(u'кооператив') or fullname.endswith(u'кооп.'):
            # full_lex = morph.parse(u'кооператив')
            full_gen = 'masc'
        else:
            full_lex = morph.parse(fullname)
            full_gen = full_lex[0].tag.gender
        if full_gen is None:
            full_gen = 'masc'
            logging.warn(("Unknown gender: ", fullname, full_lex, desc_word))
        word_lex = morph.parse(words[-1])
        word_gen = word_lex[0].tag.gender
        for full, socr in descr.iteritems():
            if desc_word in socr:
                if res:
                    # При неоднозначности сокращение имеет приоритет
                    res.insert(0, desc_word)
                desc_lex = [it for it in morph.parse(full)
                            if {'ADJF', 'nomn', 'masc'} in it.tag]
                res.append(desc_lex[0].inflect({full_gen}).word.title())
                if word_gen:
                    res.append(desc_lex[0].inflect({word_gen}).word.title())
                res.extend(socr)
        return (res, words)
    else:
        # TODO: Добавить проверку/пропуск на рассогласование
        return ((desc_word,), words)


def get_num(words, fullname):
    """Разделение слова на числительное и остальные"""
    num_word = u''
    if words[0].lower() in numerals.adj:
        num_word = words[0]
        words = words[1:]
    elif words[-1].lower() in numerals.adj:
        num_word = words[-1]
        words = words[:-1]
    if num_word:
        # TODO: Добавить проверку/пропуск на рассогласование
        return ((num_word, numerals.adj[num_word.lower()]), words)
    else:
        return ((num_word,), words)


def get_repr_tags(words):
    def possible(tag):
        if {'Surn', 'nomn'} in tag:
            return False
        if {'Name', 'nomn'} in tag:
            return False
        return True
    repr_word = words[-1]
    lexemes = morph.parse(repr_word)  # TODO: Ватин, клочков...
    lexemes = filter(lambda lex: possible(lex.tag), lexemes)
    if not lexemes:
        # Default parse contains only impossible lexemes
        lexemes = suff_morph.parse(repr_word, repr_word.lower(), set())
        tags = [lex[1] for lex in lexemes]
        res = list(filter(possible, tags))
    else:
        res = [l.tag for l in lexemes]
    # подпорочки по окончанию - иногда сокращённые прилагательные
    # от редких форм не угадываются и принимаются за существительные
    if repr_word.endswith(u'ов'):
        res.extend([l.tag for l in morph.parse(u'отцов')])
    elif repr_word.endswith(u'ин'):
        res.extend([l.tag for l in morph.parse(u'мамин')])
    return res

def check_synonym(name):
    if name in all_synonyms:
        for group in synonyms:
            if name in group:
                return group
    else:
        return (name,)


def alt_jo(words):
    yield words
    if any([(u'ё' in it) for it in words]):
        yield [it.replace(u'ё', u'е') for it in words]
    if any([(u'е' in it) for it in words]):
        res = []
        go = False
        for word in words:
            res.append(word)
            for jo_, je_ in jo.iteritems():
                if je_ in word:
                    go = True
                    res[-1] = word.replace(je_, jo_)
                    break
        if go:
            yield res


def unslash(basename):
    '''Replace slashes with parentheses'''
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
    elif fullname == u'чувашия':
        yield u"Чувашия"

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

            repr_tags = get_repr_tags(words)
            for num in nums:
                for desc in descs:
                    for words_ in alt_jo(words):
                        if any([({'ADJF'} in tag or {'ADJS'} in tag)
                                for tag in repr_tags]):
                            yield btr_join([num, desc] + words_ + [fullname, ])
                            yield btr_join([num, ] + words_ + [desc, fullname])
                        if not all([({'ADJF'} in tag) for tag in repr_tags]):
                            yield btr_join([num, fullname, desc] + words_)
                            yield btr_join([desc, num, fullname] + words_)
                            yield btr_join([num, desc, fullname] + words_)
            basename_ = u" ".join((fullname, name))

        yield basename_
