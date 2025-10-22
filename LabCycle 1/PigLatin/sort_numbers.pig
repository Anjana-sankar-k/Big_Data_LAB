numbers = LOAD 'input.txt' USING PigStorage('\n') AS (num:int);

sorted_numbers = ORDER numbers BY num ASC;

STORE sorted_numbers INTO 'output' USING PigSTorage('\t');
