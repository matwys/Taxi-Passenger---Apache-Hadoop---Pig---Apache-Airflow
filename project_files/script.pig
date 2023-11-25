REGISTER /usr/lib/pig/piggybank.jar;


passengerCounter = LOAD '$input_dir_mapreduce' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t',
'NO_MULTILINE','NOCHANGE')
as (miesiac:chararray, strefa:int,
 liczba:int);

dzielnica = LOAD '$input_dir_zone' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(',',
'NO_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER')
as (id:int,
 Borough:chararray,Zone:chararray,service_zone:chararray);

passengerCounter_with_dzielnica = JOIN passengerCounter BY strefa, dzielnica BY id;

passengerCounter_with_dzielnica_simple = FOREACH passengerCounter_with_dzielnica GENERATE
miesiac as miesiac,
 Borough as borough,
 liczba as liczba;

a = GROUP passengerCounter_with_dzielnica_simple BY (miesiac, borough);

result = foreach a generate group, SUM(passengerCounter_with_dzielnica_simple.liczba) as sum;

res = foreach result generate group.miesiac as miesiac, group.borough as borough, sum as sum;

finish = FOREACH(GROUP res BY miesiac) {
    ordered = ORDER res BY sum DESC;
    required = LIMIT ordered 3;
    GENERATE FLATTEN(required);
};

finish_simple = FOREACH finish GENERATE
miesiac as month,
 borough as borough,
 sum as passengersCount;

STORE finish_simple INTO '$output_dir' USING JsonStorage();