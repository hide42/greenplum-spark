# Load data to GP
Для выгрузки данных из спарка в гринплам сущетсвует несколько способов:
 - Внешние таблицы 
 - - gpfdist
 - - pxf (only hdfs?)
 - COPY (postgres)
 - Напрямую через драйвер
 
 Расположены в порядке от самых быстрых и масштабируемых до самых медленных и немасштабируемых соответсвенно. 
 В MPP архитектуре greenplum подразумевает загрузку данных через единую master-node, которая вычисляет хеш по distributed
 полю записи для определения сегмента для её размещения, но gpfdist и pxf реализуют прямую загрузку на сегменты, минуя master-node,
 что позволяет масштабировать пропускную способность горизонтально. Для быстрой работы gpload реализован на C++ и не имеет каких-либо
 конекторов для jvm языков.
 
 (Параллельная загрузка не гарантирует порядок данных, порядок гарантирует copy и реализации через драйвер)
 
 ## Внешние таблицы
 
 Решения через внешние таблицы требуют промежуточную запись в файловую систему(можно использовать как доп. чекпоинты?),
 а операция записи на ROM является достаточно дорогой (как и в MR). Поэтому используют named pipes(рекомендации arenadata support),
 либо через выделение RAM диска, куда и будут стекаться файлы для gpfdist. 
 
  Примеры с named pipes :
 ```
 your_program.sh | gpfdist -f /dev/stdin
 ```
 
 ```
 mkfifo /tmp/pipes/my_pipe; gpfdist -d /tmp/pipes
 ```
 
 [Пример вывода в scala в named-pipe](https://stackoverflow.com/questions/28095469/stream-input-to-external-process-in-scala)
 Сделать через pipe метод RDD ?
 
 Имеются [старые реализации gpfdist](https://github.com/spring-cloud-stream-app-starters/gpfdist) сервера gpfdist для springXD(тоже pivotal).
 (Медленно и ненадежно).
 
 Реализация через посторонние программы дает меньше контроля/мониторинга всего процесса и с архитектурной т.з. - не есть ок.
 
 ## [COPY](https://gpdb.docs.pivotal.io/43190/ref_guide/sql_commands/COPY.html)
 Copy аналогичен одноименному copy в постгрес(gp - форк pg, в 5м гринпламе postgres 8.2+).
 И в jdbc коннекторе postgresql имеется copyManager, который переводит поток байт в таблицы pg(gp).
 ```
 val copyManager = new CopyManager(conn)
 val sql = s"COPY $tableName" +
          s" FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E';'"
 val input = #INPUT STREAM
 copyManager.copyIn(sql, input)
 ``` 
 Для создания InputStream из df-rdd-iterator есть несколько способов :
  - Запись в промежуточный файл (доп.чекпоинт, но из-за операции rom медленее, но даже так в разы быстрее обычного jdbc)
  - Собрать итератор в массив и из него stream (двойная аллокация - больше памяти)
  - PipedStreams - идеально). В одном потоке пишем, в другом copyManager читает. +Промисы на таймаут
 Достаточно быстрый для малых-средних кластеров(с малым (до 10) количеством сегментов скорость copy может быть близка к gpfdist), подобным способом вы можете и читать данные, так же быстрее(х10-100 в сравнении с обычным jdbc).
 
 
  Реализацию коннектора через copy для df spark'a можете найти в этом репозитории в директории spark-copy-throw-master.
 
 ## Распределенный COPY
 GP имеет сигнатуру COPY с указанием сегмента:
 ```COPY $tableName FROM $dist-file ON SEGMENT```
 В данном случае ваше приложение должно взять на себя ответственность за распределение на сегмент по distributed полям.
 Из исходников GP узнаем алгоритм хеширования - FNV1_32, находим реализации подобного алгоритма в скале и прогоняем через
 него наши distributed поля и берем остаток от деления хеша на количество сегменов(сегмент можно узнать по 'скрытому' полю segid у каждой записи в таблице),
 затем сравниваем с сегментом записи в gp - не сходится, переписываем реализацию с C на scala - получаем нужный результат, делаем свой partitioner:
 ```
def getPartition(key: Any): Int = {
    val inpString = key.toString
    val fnvPrime = 16777619
    var digest = -2128831035
    var i = 0
    while (i < inpString.length) {
      digest = digest*fnvPrime
      digest = digest^(inpString.substring(i, i + 1).charAt(0).toInt & 255)
      
      i += 1; i - 1
    }
    var seg_id=digest%numPartition
    if(seg_id<0)
      seg_id+=numPartition
    seg_id
  }
 ```
 Так каждая партиция rdd будет готова к copy-on-segment(Так же для более рационального использования параллельности(имеются затраты на установления соединения) можно сделать
 доп. партиции для самого df-spark с указанием segid, для того чтобы спарк сделал выгрузку на сегменты за раз => запоминаем seg_id
 , делим seg_id на уровень параллелизма екзекьюторов). Copy-on-segment не работает с stdin и требует директорию с файлами по некоторой маске где и указан segid
 , так что снова два варианта:
  - записать всё спарком по маске с seg_id в файлы и только потом вызывать copy on segment (чекпоинты, полный контроль и т п)
  - записывать параллельно в пайпы с вызовом copy on segment (быстро)
  - записать спарком всё в ram директории ?
  
  Стоит дополнить, что copy on segment рекомендуется использоваться только 'профессионалам' из-за своих нюансов - если у вас составное distributed поле(наприм. для джоина по нескольким полям),
  то алгоритм вычисления хеша будет сложнее - с запоминанием пред.состояния, если сегмент переедет на зеркало - всё потеряется, если сегмент указан не верно файл потеряется(с апдейтом алгоритм может поменяться, так в 6гп он вроде другой).
  
  Данная реализация(как и все другие)) подразумевает выбор между скоростью, надежностью и сложностью реализации.
  
