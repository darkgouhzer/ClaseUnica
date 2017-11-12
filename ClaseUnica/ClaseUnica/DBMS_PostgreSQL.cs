using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.Data;
using System.Windows.Forms;
namespace UnicaSQL
{
    public class DBMS_PostgreSQL
    {
        NpgsqlConnection conexion = new NpgsqlConnection();
        NpgsqlCommand cmd;
        NpgsqlDataAdapter Adapter;
        //conexionTxt txt;
        public DBMS_PostgreSQL(string sCadConn)
        {
            conexion.ConnectionString = sCadConn;
        }
        public Boolean conectarse()
        {
            Boolean bAllOk = false;
            try
            {
                conexion.Open();
                bAllOk = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
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
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
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
                    cmd = new NpgsqlCommand(comando.Replace("'","\""), conexion);
                    if (transOK)
                    {
                        cmd.Transaction = this.tran;
                    }
                    int n = cmd.ExecuteNonQuery();
                    bAllOk = true;
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
                    cmd = new NpgsqlCommand(sSQLStmt, conexion);
                    Adapter = new NpgsqlDataAdapter();
                    if (transOK)
                    {
                        cmd.Transaction = this.tran;
                    }
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
        NpgsqlTransaction tran;      
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
