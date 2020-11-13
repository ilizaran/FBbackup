# MIT License

# Copyright (c) 2020 Ignacio José Lizarán Rus

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import fdb
from progress.bar import Bar

class FbBackup:

    def __init__(self, parsed_args):

        self.con = fdb.connect(
                dsn=parsed_args['dsn'],
                user=parsed_args['user'], 
                password=parsed_args['password'],
                sql_dialect=parsed_args['sql_dialect'], 
                charset=parsed_args['charset']
        )
        self.cur = self.con.cursor()

        sql =   '''
                    SELECT a.RDB$RELATION_NAME
                    FROM RDB$RELATIONS a
                    where left(a.RDB$RELATION_NAME,4) <> 'RDB$'
                    and left(a.RDB$RELATION_NAME,4) <> 'MON$'
                    and left(a.RDB$RELATION_NAME,4) <> 'SEC$'
                '''

        self.cur.execute(sql)
        self.tables = [item[0].strip() for item in self.cur.iter()]


    def set_table(self, table):
        if table in self.tables:
            self.tables=[table, ]
        else:
            raise Exception("Table doesn't exist") 
    
    def get_fields_names_types(self, table):
        sql =   '''
                select rf.RDB$FIELD_NAME, f.RDB$FIELD_TYPE, t.RDB$TYPE_NAME  from rdb$relation_fields rf
                inner join RDB$FIELDS f on rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
                inner join RDB$TYPES t on f.RDB$FIELD_TYPE = t.RDB$TYPE and t.RDB$FIELD_NAME ='RDB$FIELD_TYPE'
                where rdb$relation_name='%s' order by RDB$FIELD_POSITION
                ''' % table

        self.cur.execute(sql)
        return [(item[0].strip(),item[1], item[2].strip()) for item in self.cur.iter()]


    def get_field_str(self, value, type):
        '''
        Return value string formated to insert

        Types from Firebird: 
            SELECT r.RDB$DB_KEY, r.RDB$FIELD_NAME, r.RDB$TYPE, r.RDB$TYPE_NAME, r.RDB$DESCRIPTION, r.RDB$SYSTEM_FLAG
            FROM RDB$TYPES r
            WHERE RDB$FIELD_NAME ='RDB$FIELD_TYPE'
                7	SHORT                          
                8	LONG                           
                9	QUAD                           
                10	FLOAT                          
                12	DATE                           
                13	TIME                           
                14	TEXT                           
                16	INT64                          
                23	BOOLEAN                        
                27	DOUBLE                         
                35	TIMESTAMP                      
                37	VARYING                        
                40	CSTRING                        
                45	BLOB_ID                        
                261	BLOB                           

        '''
        if value == None:
            return 'NULL'

        QUOTATION_TYPES = (14, 37, 35, 40, 12, 13, 261)
        if type in QUOTATION_TYPES:
            return "'%s'" % str(value).replace("'","\\'")
        
        return str(value)

    def get_rowcount(self, table):
        sql = 'SELECT count(*) FROM "%s"' % table
        self.cur.execute(sql)
        return self.cur.fetchone()[0]


    def backup_table(self, table, file):
        fields_names = self.get_fields_names_types(table)
        fields_txt = ",".join(['%s' % item[0] for item in fields_names])
        insert_txt = 'INSERT INTO {0} ({1}) VALUES'.format(table,fields_txt)
        
        rowcount = self.get_rowcount(table)

        if rowcount == 0:
            return

        sql = 'SELECT * FROM "%s"' % table
        self.cur.execute(sql)

        bar = Bar('Backup table: %s' % table, max=rowcount)
        for row in self.cur.itermap():
            list_fields_txt = [self.get_field_str(row[field_name[0]],field_name[1]) for field_name in fields_names]
            fields_txt = ",".join(list_fields_txt)
            file.write('{0} ({1});\n'.format(insert_txt, fields_txt))
            bar.next()
        bar.finish()


    def backup_tables(self):
        for table in self.tables:
            file = open('%s.sql' % table,"w")
            self.backup_table(table, file)
            file.close()

