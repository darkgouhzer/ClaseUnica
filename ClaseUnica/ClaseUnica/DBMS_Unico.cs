using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace UnicaSQL
{
    public class DBMS_Unico
    {
        string sSGBD = "";
        string sConexion = "";
        DBMS_SQLServer sql;
        DBMS_MySQL MySQL;
        DBMS_PostgreSQL PosgretSQL;

        public DBMS_Unico(string sSGBDn, string sServer, string sBD, string sUsuario, string sPassword, Int32 sPuerto)
        {
            sSGBD = sSGBDn;
            switch (sSGBD)
            {
                case "SQL_Server":
                    sConexion = string.Format("Data Source={0};Initial Catalog={1};Integrated Security=True;", sServer, sBD);
                    sql = new DBMS_SQLServer(sConexion);
                    break;

                case "MySQL":
                    sConexion = string.Format("server={0}; database={1}; Uid={2}; pwd={3}; AllowUserVariables=True;", sServer, sBD, sUsuario, sPassword);
                    MySQL = new DBMS_MySQL(sConexion);
                    break;

                case "PostgreSQL":
                    sConexion = string.Format("server={0}; database={1}; user id={2}; Password={3};", sServer, sBD, sUsuario, sPassword);
                    PosgretSQL = new DBMS_PostgreSQL(sConexion);
                    break;
            }
        }
        public Boolean Conectarse()
        {
            Boolean bALLOK = false;
            try
            {
                switch (sSGBD)
                {
                    case "SQL_Server":

                        bALLOK = sql.Conectarse();

                        break;

                    case "MySQL":

                        bALLOK = MySQL.conectarse();
                        break;

                    case "PostgreSQL":
                        bALLOK = PosgretSQL.conectarse();
                        break;
                }
                //bALLOK = true;

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bALLOK;
        }
        public Boolean Desconectarse()
        {
            Boolean bALLOK = false;
            try
            {
                switch (sSGBD)
                {
                    case "SQL_Server":
                        sql.Desconectarse();
                        break;

                    case "MySQL":
                        MySQL.desconectarse();
                        break;

                    case "PostgreSQL":
                        PosgretSQL.desconectarse();
                        break;
                }
                bALLOK = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bALLOK;
        }
        public Boolean Ejecutar(string sSQLstmt, ref DataTable Tabla)
        {
            Boolean bAllOk = false;
            try
            {
                switch (sSGBD)
                {
                    case "SQL_Server":
                        bAllOk = sql.Ejecutar(sSQLstmt, ref Tabla);
                        break;

                    case "MySQL":
                        bAllOk = MySQL.Ejecutar(sSQLstmt, ref Tabla);
                        break;

                    case "PostgreSQL":
                        bAllOk = PosgretSQL.Ejecutar(sSQLstmt, ref Tabla);
                        break;
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }


        public Boolean Ejecutar(string sSQLStmt)
        {
            Boolean bAllOk = false;
            try
            {
                switch (sSGBD)
                {
                    case "SQL_Server":
                        bAllOk = sql.Ejecutar(sSQLStmt);
                        break;

                    case "MySQL":
                        bAllOk = MySQL.ejecutar(sSQLStmt);
                        break;

                    case "PostgreSQL":
                        bAllOk = PosgretSQL.ejecutar(sSQLStmt);
                        break;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }

        public Boolean IniciarTransaccion()
        {
            Boolean bAllOk = false;
            try
            {
                switch (sSGBD)
                {
                    case "SQL_Server":
                        sql.IniciarTransaccion();
                        bAllOk = true;
                        break;

                    case "MySQL":
                        MySQL.IniciarTransaccion();
                        bAllOk = true;
                        break;

                    case "PostgreSQL":
                        PosgretSQL.IniciarTransaccion();
                        bAllOk = true;
                        break;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }
        public Boolean FinTransaccion(Boolean bAllOk)
        {
            try
            {
                switch (sSGBD)
                {
                    case "SQL_Server":
                        sql.FinTransaccion(bAllOk);
                        break;

                    case "MySQL":
                        MySQL.FinTransaccion(bAllOk);
                        break;

                    case "PostgreSQL":
                        PosgretSQL.FinTransaccion(bAllOk);
                        break;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }
    }
}

