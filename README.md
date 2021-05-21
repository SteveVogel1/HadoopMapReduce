# HadoopMapReduce


# Aufgabe 1: Verkaufsanalyse

**Verarbeitung Inputdatei**

Die Map Funktion wird für jede Zeile aufgerufen. Die Zeile wird dann anhand von Tabulator-Zeichen aufgetrennt. Die Zeit ist dabei der 2. Wert (im Array der Werte der Zeile). Die Zeit wird danach anhand des : (Doppelpunkts) aufgetrennt und anschliessend wird die Stunde in ein Long umgewandelt.
Auch der Betrag wird nach selbem Vorgehen extrahiert und in ein Double umgewandelt.

**Output Map Funktion

Die Ausgabe der Map-Funktion ist ein Key-Value-Pair für jede Textzeile:

* Die Stunde als Key (LongWritable)
* Der Betrag als Value (DoubleWritable)

** Reduce Funktion **

Die Reduce Funktion summiert alle Beträge derselben Stunde auf und berechnet damit den Durchschnitts Einkaufsbetrag.
Ausgabe der Reduce Funktion ist folgendes Key-Value-Pair:

* Stunde als Key (CustomLongWritable)
* Durchschnitts-Einkaufsbetrag als Value (Double)

`CustomLongWritable` ist eine Subklasse von `LongWritable` und überschreibt die `toString` Methode. So können wir nicht nur X für die Stunde sondern auch noch ein Label ausgeben.

** CustomPartitioner **
Bei der Verwendung von mehreren Reducern ist uns aufgefallen, dass die Ausgabe dann nicht mehr sortiert ist. Wir haben herausgefunden, dass pro Reducer am Schluss eine Datei erstellt wird. Die Werte sind zwar innerhalb der Datei sortiert, aber nicht über mehrere Dateien hinweg. Der verwendete `HashPartitioner` stellt sich als Problem heraus. Deshalb haben wir einen eigenen Partition `CustomPartitioner` erstellt. Dieser weist der ersten Partition die tiefsten Keys (Stunden) zu und dem letzten Partitioner die grössten. So ist am Ende die Ausgabe wieder aufsteigend nach Stunden sortiert.
Die Funktion für das Berechnen, an welche Partition ein Key hinzugewiesen wird, hat aber einige kleine Nachteile:

* Die Werte werden nicht gleichmässig wie beim HashPartition auf die Reducer verteilt. Wir haben dass bereits ein wenig behoben, indem wir davon ausgehen, dass zwischen 00:00 und 06:00 sowie 20:00 und 24:00 keine Verkäufe durchgeführt werden. Auch wenn unsere gewählte Funktion nicht optimal ist, kann so die Leistung durch mehrere Reducer erhöht werden und gleichzeitig die Ausgabereihenfolge beibehalten werden. 

Die könnte man natürlich auch noch nachträglich beheben in dem man die Textdateien z.B. mit Commandlinetools sortiert.

<span style="background:yellow;">TODO Auszug Ausgabe</span>