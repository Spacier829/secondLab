import sqlite3
import re
import bs4
import requests


class Searcher:

    # 0. Конструктор
    def __init__(self, dbFileName):
        # Подключение к файлу БД
        self.connection = sqlite3.connect(dbFileName)

    # 0. Деструктор
    def __del__(self):
        # Закрытие соединения с БД
        self.connection.close()

    # 1. Получение id для слов в запросе
    def getWordsIds(self, queryString):
        # Приведение строки к нижнему регистру
        queryString = queryString.lower()

        # Разделение на отдельные слова
        queryWordsList = queryString.split(" ")

        # Список для хранения wordId
        wordsIdList = list()

        for word in queryWordsList:
            # Для каждого слова делается запрос на позицию в БД
            sqlSelect = "SELECT rowid FROM wordList WHERE word = \"{}\" LIMIT 1;".format(
                word)
            wordId = self.connection.execute(sqlSelect).fetchone()[0]
            if wordId != None:
                # Если такое слово имеется, то его id помещается в список
                wordsIdList.append(wordId)
                print("Слово:", word, "id:", wordId)
            else:
                # Если слово не найдено, тогда исключение
                raise Exception(
                    "Одно из слов поискового запроса не найдено:" + word)
        return wordsIdList

    # 2. Поиск комбинаций из всех искомых слов в проиндексированных url-адресах
    def getMatchRows(self, queryString):
        # Приведение строки к нижнему регистру
        queryString = queryString.lower()
        # Разделение на отдельные слова
        queryWordsList = queryString.split(" ")
        # Список для хранения wordId
        wordsIdList = self.getWordsIds(queryString)

        # Переменная для полного sql-запроса
        sqlFullQuerry = ""

        # Объекты-списки для дополнений sql-запроса
        sqlPart_ColumnName = list()  # Имя столбца
        sqlPart_Join = list()  # INNER JOIN
        sqlPart_Condition = list()  # WHERE

        # Конструктор sql-запроса
        # Обход в цикле каждого слова и добавление его в sql-запрос
        for wordIndex in range(0, len(queryWordsList)):
            # Получение id слова
            wordId = wordsIdList[wordIndex]

            if wordIndex == 0:
                # Обязательная часть для первого слова
                sqlPart_ColumnName.append(
                    """w0.fk_urlId fk_urlId --идентификатор url-адреса""")
                sqlPart_ColumnName.append(
                    """  , w0.location w0_loc --положение первого искомого слова""")
                sqlPart_Condition.append(
                    """WHERE w0.fk_wordId={} --совпадение w0 с первым словом""".format(wordId))
            else:
                # Доп часть для >=2 искомых слов
                if len(queryWordsList) >= 2:
                    sqlPart_ColumnName.append(
                        "  , w{}.location w{}_loc --положение следующего искомго слова".format(wordIndex, wordIndex))

                # Добавление INNER JOIN
                sqlPart_Join.append(
                    "INNER JOIN wordLocation w{} on w0.fk_urlId=w{}.fk_urlId".format(wordIndex, wordIndex, wordIndex))
                # Добавление ограничивающего условия
                sqlPart_Condition.append(
                    " AND w{}.fk_wordId={} --совпадение w{} с соответствующим словом".format(wordIndex, wordId, wordIndex))

        # Объединение запроса из отдельных частей
        sqlFullQuerry += "SELECT"

        for sqlPart in sqlPart_ColumnName:
            sqlFullQuerry += "\n"
            sqlFullQuerry += sqlPart

        # Обязательная часть таблица источник
        sqlFullQuerry += "\n"
        sqlFullQuerry += "FROM wordLocation w0 "

        # Часть для объединения таблицы INNER JOIN
        for sqlPart in sqlPart_Join:
            sqlFullQuerry += "\n"
            sqlFullQuerry += sqlPart

        # Обязательная часть и дополнения для блока WHERE
        for sqlPart in sqlPart_Condition:
            sqlFullQuerry += "\n"
            sqlFullQuerry += sqlPart

        # Выполнение sql-запроса и извлечение ответа от БД
        print(sqlFullQuerry)
        cursor = self.connection.execute(sqlFullQuerry)
        rows = [row for row in cursor]
        return rows, wordsIdList

    # 3. Метод нормализации
    def normalizeScores(self, scores, smallIsBetter=0):
        # Словарь с результатом
        resultDict = dict()

        # Малая величина для деления на 0
        vSmall = 0.00001

        minScore = min(scores.values())
        maxScore = max(scores.values())

        # Перебор каждой пары ключ значение
        for (key, val) in scores.items():
            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # Ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minScore) / max(vSmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # Вычисление ранга как доли от макс.
                # Ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxScore

        return resultDict

    # 4. Метод ранжирования - расположение в документе
    def locationScore(self, rowsLoc):
        # Словарь с расположением от начала страницы упоминаний искомых слов
        locationsDict = dict([(row[0], 1000000) for row in rowsLoc])
        for row in rowsLoc:
            # Получение всех позиций искомых слов кроме нулевого
            loc = sum(row[1:])
            # Проверка, является ли найденная комбинация ближе к началу, чем предыдущие
            if loc < locationsDict[row[0]]:
                locationsDict[row[0]] = loc
        return self.normalizeScores(locationsDict, smallIsBetter=1)

    # 5. Получение URL-адреса
    def getUrlName(self, id):
        return self.connection.execute("SELECT url FROM urlList where rowId = {};".format(id)).fetchone()[0]

    # 6. Формирование списка url, вычисление ранга, вывод сортировки
    def getSortedList(self, queryString):
        # Получение списка вхождения слов
        rowsLoc, wordIds = self.getMatchRows(queryString)
        # Результаты ранжирования рангов
        locationScores = self.locationScore(rowsLoc)
        pageRankScores = self.pageRankScore(rowsLoc)

        # Список для общего ранга
        m1Scores = dict([(row[0], 0) for row in rowsLoc])

        #
        weights = [(1.0, locationScores),
                   (1.0, pageRankScores)]

        for (weight, scores) in weights:
            for url in m1Scores:
                m1Scores[url] += weight*scores[url]

        # Список для последующей сортировки рангов и url-адресов
        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append(pair)

        # Сортировка из словаря по убыванию
        rankedScoresList.sort(reverse=1)

        print("urlId, pr_score, loc_score, sum_score, Url")
        for (score, urlId) in rankedScoresList[0:20]:
            print("{:>3} {:>10.2f} {:>10.2f} {:>10.2f}   {}".format(
                urlId, pageRankScores[urlId], locationScores[urlId], score, self.getUrlName(urlId)))
        self.getHtmlCode(self.getUrlName(33))
        self.createMarkedHtmlFile(
            'html.html', self.getUrlName(33), queryString)
    # 7. Рассчет pageRank

    def calculatePageRank(self, iterations=5):
        # Подготовка БД
        # Удаление текущего содержимого таблцы pageRank
        self.connection.execute("DROP TABLE IF EXISTS pageRank;")
        self.connection.execute("""CREATE TABLE IF NOT EXISTS pageRank(
                        rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                        url INTEGER,
                        score REAL);""")

        # Для некоторых столбцов в таблицах БД укажем команду создания объекта "INDEX" для ускорения поиска в БД
        self.connection.execute("DROP INDEX IF EXISTS wordidx;")
        self.connection.execute("DROP INDEX IF EXISTS urlidx;")
        self.connection.execute("DROP INDEX IF EXISTS wordurlidx;")
        self.connection.execute("DROP INDEX IF EXISTS urltoidx;")
        self.connection.execute("DROP INDEX IF EXISTS urlfromidx;")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS wordidx ON wordList(word)")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS urlidx ON urlList(url)")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS wordurlidx ON wordLocation(fk_wordId)")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS urltoidx ON linkBetweenurl(fk_toUrlId)")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS urlfromidx ON linkBetweenurl(fk_fromUrlId)")
        self.connection.execute("DROP INDEX IF EXISTS rankurlididx;")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS rankurlididx ON pagerank(url)")
        self.connection.execute("REINDEX wordidx;")
        self.connection.execute("REINDEX urlidx;")
        self.connection.execute("REINDEX wordurlidx;")
        self.connection.execute("REINDEX urltoidx;")
        self.connection.execute("REINDEX urlfromidx;")
        self.connection.execute("REINDEX rankurlididx;")

        # В начальный момент времени ранг для всех url==1
        self.connection.execute(
            "INSERT INTO pageRank (url, score) SELECT url, 1.0 FROM urlList")
        self.connection.commit()

        # Вычисление pagerank
        for i in range(iterations):
            print("Итерация %d" % (i))
            for (url,) in self.connection.execute("SELECT url from urlList"):
                pr = 0.15
                # Обход всех страниц, ссылающихся на данную
                for (linker,) in self.connection.execute("SELECT DISTINCT fk_fromUrlId FROM linkBetweenUrl where fk_toUrlId='{}'".format(url)):
                    # Поиск ранга ссылающейся страницы
                    linkingpr = self.connection.execute(
                        "SELECT score FROM pageRank WHERE url='{}'".format(linker)).fetchone()[0]

                    # Поиск общего числа ссылок на ссылающейся странице
                    linkingCount = self.connection.execute(
                        "SELECT COUNT(*) FROM linkBetweenUrl where fk_fromUrlId='{}'".format(linker)).fetchone()[0]
                    pr += 0.85*(linkingpr/linkingCount)
                self.connection.execute(
                    "UPDATE pageRank SET score=%f where url='%s'" % (pr, url))
        self.connection.commit()

    # 8. Извлечение ранга
    def pageRankScore(self, rows):
        # Получение значений pageRank
        pageranks = dict([(row[0], self.connection.execute(
            "SELECT score FROM pageRank where rowId=%d" % row[0]).fetchone()[0]) for row in rows])
        # Нормализация ранга
        normalizedScores = self.normalizeScores(pageranks, smallIsBetter=1)
        return normalizedScores

    # 9. Создание html-файла
    def createMarkedHtmlFile(self, markedHtmlFileName, testText, testQuery):
        # Преобразование текста к нижнему регистру
        testText = testText.lower()
        testQueryWordsList = testQuery.split(" ")
        for i in range(0, len(testQueryWordsList)):
            testQueryWordsList[i] = testQueryWordsList[i].lower()

        # Получение текста страницы с знаками переноса строк и препинания.
        # Использование регулярок
        wordList = re.compile("[\\w]+|[\\n.,!?:—]").findall(testText)

        # Получение html-кода с маркировкой искомых слов
        htmlCode = self.getMarkedHtml(wordList, testQueryWordsList)
        print(htmlCode)

        # Сохранение html-кода в файл с указанным именем
        file = open(markedHtmlFileName, 'w', encoding="utf-8")
        file.write(htmlCode)
        file.close()

    # 10. Генерация html-кода с маркировкой указанных слов цветом
    def getMarkedHtml(self, wordList, queryList):
        # Переменная-заготовка для html-кода
        resutlHtml = ""
        resutlHtml += "<!DOCTYPE HTML>"
        for word in wordList:
            if word in queryList:
                wordidx = queryList.index(word)
                if wordidx == 0:
                    color = "red"
                if wordidx == 1:
                    color = "cyan"
                resutlHtml += "<span style=\"background-color:"+color+";\">"
                resutlHtml += ''.join(word)
                resutlHtml += "</span>"
            elif word == '\n':
                resutlHtml += "<br>"
            else:
                resutlHtml += ''.join(word)
            resutlHtml += " "
        return resutlHtml

    # ==========================

    def getHtmlCode(self, url):
        htmlDoc = requests.get(url).text
        soup = bs4.BeautifulSoup(htmlDoc, "html.parser")
        soup.prettify()
        listUnwantedItems = ['script', 'style', 'data-item']
        for script in soup.find_all(listUnwantedItems):
            script.decompose()
        soupText = soup.find('body').get_text()
        return soupText

# ------------------------------------------
# Основная функция


def main():
    test = Searcher("test.db")
    search = "туризм россии"
    rowsLoc, wordsIdList = test.getMatchRows(search)
    print("========================")
    print(search)
    print(wordsIdList)
    test.calculatePageRank()
    print("urlid, loc_w0, loc_w1")
    for loc in rowsLoc:
        print(loc)
    test.getSortedList(search)
# --------------------------------------------


main()
