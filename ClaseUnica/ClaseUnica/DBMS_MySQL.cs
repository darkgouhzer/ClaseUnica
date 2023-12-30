using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;
using MySql.Data;
using System.Windows.Forms;
namespace UnicaSQL
{
    public class DBMS_MySQL
    {
        MySqlConnection conexion = new MySqlConnection();
        MySqlCommand cmd;
        MySqlDataAdapter Adapter;
        public Boolean OpenConnection;

        public DBMS_MySQL(string sCadConn)
        {
            conexion.ConnectionString = sCadConn;
        }
        public Boolean conectarse()
        {
            Boolean bAllOk = false;
            try
            {
                conexion.Open();
                OpenConnection = bAllOk = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                OpenConnection = false;
            }
            return bAllOk;
        }
        public Boolean desconectarse()
        {
            Boolean bAllOk = false;
            try
            {
                conexion.Close();
                bAllOk = true;
                OpenConnection = false;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                OpenConnection = false;
            }
            return bAllOk;
        }
        public Boolean ejecutar(string comando)
        {
            Boolean bAllOk = false;
            try
            {
                if (conexion.State == ConnectionState.Open)
                {
                    cmd = new MySqlCommand(comando, conexion);                    
                    if (transOK)
                    {
                        cmd.Transaction = this.tran;
                    }
                    int n = cmd.ExecuteNonQuery();
                    if (n > 0)
                    {
                        bAllOk = true;
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }
        public Boolean Ejecutar(String sSQLStmt, ref DataTable Tabla)
        {
            Boolean bAllok = false;
            try
            {
                if (conexion.State == ConnectionState.Open)
                {
                    cmd = new MySqlCommand(sSQLStmt, conexion);
                    if (transOK)
                    {
                        cmd.Transaction = this.tran;
                    }
                    Adapter = new MySqlDataAdapter();

                    Adapter.SelectCommand = cmd;

                    DataSet ds = new DataSet();

                    int n = Adapter.Fill(ds);
                    if (n > 0)
                    {
                        Tabla = ds.Tables[0];
                    }
                    bAllok = true;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllok;
        }
        Boolean transOK = false;
        MySqlTransaction tran;
        public void IniciarTransaccion()
        {
            tran = conexion.BeginTransaction();
            transOK = true;
        }
        public void FinTransaccion(Boolean bAllOk)
        {
            if (bAllOk)
            {
                tran.Commit();
            }
            else
            {
                tran.Rollback();               
            }
            transOK = false;
        }
    }
}
