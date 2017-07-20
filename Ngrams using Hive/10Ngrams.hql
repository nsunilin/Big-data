drop table if exists Book;
create table if not exists Book(line string) partitioned by (bookName string);

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/A mid summer night dream.txt' INTO table Book partition(bookName='A mid summer night dream.txt');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/Hamlet.txt' INTO table Book partition(bookName='Hamlet');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/King Richard III.txt' INTO table Book partition(bookName='King Richard III');


LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/MacBeth.txt' INTO table Book partition(bookName='MacBeth');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/Othello.txt' INTO table Book partition(bookName='Othello');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/Romeo and Juliet.txt' INTO table Book partition(bookName='Romeo and Juliet');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/The Merchant of Venice.txt' INTO table Book partition(bookName='The Merchant of Venice');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/The tempest.txt' INTO table Book partition(bookName='The tempest');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/The tragedy of King Lear.txt' INTO table Book partition(bookName='The tragedy of King Lear');

LOAD DATA LOCAL INPATH '/home/administrator/Downloads/s/books/The tragedy of Julius Casear.txt' INTO table Book partition(bookName='The tragedy of Julius Casear');

select explode(NGRAMS(SENTENCES(LOWER(line)),3,10)) as trigrams from Book;

