using System;
using System.Collections.Generic;
using System.Text;

namespace MobileSMARTS_BPMConnector_ClientSide
{
    /// <summary>
    /// Пример клиентской реализации коннектора к внешней системе, 
    /// которая позволяет программисту задавать параметры этого коннектора в панели управления Mobile SMARTS.
    /// </summary>
    [Cleverence.Warehouse.Design.DisplayTypeName("BPMOnlineConnector")]
    public class BPMConnector : Cleverence.Connectivity.IConnector
    {
        #region IConnector Members

        private string id;
        [System.ComponentModel.Description("Идентификатор")]
        public string Id
        {
            get { return this.id; }
            set { this.id = value; }
        }

        private bool initialized = false;
        [System.ComponentModel.Browsable(false)]
        public bool Initialized
        {
            get { return this.initialized; }
            set { this.initialized = value; }
        }

        [System.ComponentModel.Browsable(false)]
        public bool Enabled
        {
            get;
            set;
        }


        /// <summary>
        /// Ограничение на время выполнения вызова.  Если время обработки InvokeMethod вревысит Timeout в секундах, то 
        /// сервер Mobile SMARTS поступит в соответствии с TimeoutBehavaior.
        /// </summary>
        [System.ComponentModel.Description("Тайм-аут в миллисекундах, 0 - тайм-аут не используется")]
        public int Timeout
        {
            get;
            set;
        }



        private Cleverence.Connectivity.TimeoutBehavaior timeoutBehavaior = Cleverence.Connectivity.TimeoutBehavaior.ThrowException;
        /// <summary>
        /// При превышении времени ожидания выполнения вызова сервер Mobile SMARTS сгенерирует исключение.
        /// Вторая альтернатива - прервать и попробовать вызов еще раз (Cleverence.Connectivity.TimeoutBehavaior.ReInvoke).
        /// </summary>
        [System.ComponentModel.Editor("Cleverence.Warehouse.Design.TimeoutBehavaiorTypeEditor, Cleverence.MobileSMARTS.Design",
            typeof(System.Drawing.Design.UITypeEditor))]
        [System.ComponentModel.TypeConverter("Cleverence.Warehouse.Design.TimeoutBehavaiorTypeConverter, Cleverence.MobileSMARTS.Design")]
        public string TimeoutBehavaior
        {
            get { return this.timeoutBehavaior.ToString(); }
            set
            {
                this.timeoutBehavaior = (Cleverence.Connectivity.TimeoutBehavaior)Enum.Parse(typeof(Cleverence.Connectivity.TimeoutBehavaior), value, true);
            }
        }

        #endregion

        /*#region Параметры подключения к БД BPMOnline

        /// <summary>
        /// Возвращает или устанавливает значение кастомного строкового параметра.
        /// </summary>       
        private string dataSource = "";
        [System.ComponentModel.Description("Сервер базы данных")]
        public string DataSource
        {
            get { return this.dataSource; }
            set { this.dataSource = value; }
        }
        private string initialCatalog = "M_TransLine";
        [System.ComponentModel.Description("Наименование базы данных")]
        public string InitialCatalog
        {
            get { return this.initialCatalog; }
            set { this.initialCatalog = value; }
        }
        private string integratedSecurity;
        [System.ComponentModel.Description("Проверка подлинности (True/False)")]
        [System.ComponentModel.TypeConverter("Cleverence.Warehouse.Design.IntegratedSecurity, Cleverence.Warehouse.Com.Design")]
        public string IntegratedSecurity
        {
            get { return this.integratedSecurity; }
            set { this.integratedSecurity = value; }
        }
        private string user;
        [System.ComponentModel.Description("Имя пользователя")]
        public string User { get { return this.user; } set { this.user = value; } }
        private string password;
        [System.ComponentModel.Description("Пароль")]
        [System.ComponentModel.TypeConverter("Cleverence.Warehouse.Design.PasswordConverter, Cleverence.Warehouse.Com.Design")]
        public string Password { get { return this.password; } set { this.password = value; } }
        #endregion */

        #region IConnector Members


        public string GetDisplayTextForAction(Cleverence.Warehouse.InvokeMethodAction action)
        {
            return "Функция BPMOnline";
        }

        public Type GetResultType(Cleverence.Warehouse.InvokeMethodAction action)
        {
            return null;
        }


        #endregion
    }
}
