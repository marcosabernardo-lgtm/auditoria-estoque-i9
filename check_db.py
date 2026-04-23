import sqlite3
conn = sqlite3.connect('auditoria_i9.db')
c = conn.cursor()
c.execute('SELECT name FROM sqlite_master WHERE type="table"')
tables = c.fetchall()
print('Tabelas:', [t[0] for t in tables])
if 'inventario_ciclos_historico' in [t[0] for t in tables]:
    c.execute('SELECT * FROM inventario_ciclos_historico')
    rows = c.fetchall()
    print('Dados no histórico:', rows)
else:
    print('Tabela não existe')
conn.close()