using System;
using System.IO;
using System.Net;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Text.RegularExpressions;

namespace MobileSMARTS_BPMConnector_ServerSide
{
    /// <summary>
    /// Пример серверной реализации коннектора к внешней системе, 
    /// которая позволяет серверу Mobile SMARTS обращаться к другим базам данных.
    /// </summary>
    public class BPMConnector : Cleverence.Connectivity.IConnector
    {

        // Строка адреса BPMonline сервиса OData.
        private const string serverUri = "http://WIN-B9GBQCMSFD9:7400/0/ServiceModel/EntityDataService.svc/";
        private const string authServiceUri = "http://WIN-B9GBQCMSFD9:7400/ServiceModel/AuthService.svc/Login";
        //private const string serverUri = "http://pashchenko:7400/1/ServiceModel/EntityDataService.svc/";
        //private const string authServiceUri = "http://pashchenko:7400/ServiceModel/AuthService.svc/Login";

        private static CookieContainer bpmCookieContainer = new CookieContainer();

        // Ссылки на пространства имен XML.
        private static readonly string ds = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        private static readonly string dsmd = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
        private static readonly string atom = "http://www.w3.org/2005/Atom";


        // выполнения forms-аутентификации с помощью аутентификационого сервиса bpm'online AuthService.svc.
        static void FormsAuthRequest()
        {
            // Создание экземпляра запроса к сервису аутентификации.
            var authRequest = HttpWebRequest.Create(authServiceUri) as HttpWebRequest;
            // Определение метода запроса.
            authRequest.Method = "POST";
            // Определение типа контента запроса.
            authRequest.ContentType = "application/json";

            // Инициализация учетных данных пользователя.
            string userName = "Supervisor";
            string userPassword = "Rosivrepus";
            //string userPassword = "Supervisor";

            // Включение использования cookie в запросе.
            authRequest.CookieContainer = bpmCookieContainer;

            // Помещение в тело запроса учетной информации пользователя.
            using (var requestStream = authRequest.GetRequestStream())
            {
                using (var writer = new StreamWriter(requestStream))
                {
                    writer.Write(@"{
						""UserName"":""" + userName + @""",
						""UserPassword"":""" + userPassword + @"""
					}");
                }
            }
            // Получение ответа от сервера. Если аутентификация проходит успешно, в объект bpmCookieContainer будут 
            // помещены cookie, которые могут быть использованы для последующих запросов.
            using (var response = (HttpWebResponse)authRequest.GetResponse())
            {
                if (bpmCookieContainer.Count <= 0)
                {
                    Cleverence.Log.Write("Неудачная авторизация в системе BPMOnline");
                }
            }
        }

        public BPMConnector()
        {
            this.xmls = new Cleverence.DataCollection.Xml.XmlSerializer();
            this.xmls.TypeProvider.Add(typeof(Cleverence.Warehouse.Environment));
        }

        private Cleverence.DataCollection.Xml.XmlSerializer xmls;

        #region IConnector Members

        private string id;
        public string Id
        {
            get { return this.id; }
            set { this.id = value; }
        }

        /// <summary>
        /// Возвращает или устанавливает значение кастомного строкового параметра.
        /// </summary>
        /*public int MyStringParam1
        {
            get;
            set;
        }*/

        public void Initialize()
        {
            this.initialized = true;
        }

        private bool initialized = false;
        public bool Initialized
        {
            get { return this.initialized; }
        }

        public bool Enabled
        {
            get;
            set;
        }

        //  Распределение упаковок по отрезкам
        private void recalcPackingCargo(List<string> requestListId)
        {
            foreach (var requestId in requestListId)
            {
                var getPackingIdString = "select TOP(1) Id from SxPackingCargo where SxRequestId='" + requestId + "' ";

                string PackingId = "";
                using (SqlCommand Packing = new SqlCommand(getPackingIdString, dbConnection))
                {
                    using (SqlDataReader id = Packing.ExecuteReader())
                    {
                        while (id.Read())
                        {
                            PackingId = id.GetValue(0).ToString();
                        }
                    }
                }
                if (PackingId == "") continue;

                // Создание экземпляра запроса для изменения фасовки с заданным идентификатором.
                var request = HttpWebRequest.Create(serverUri + "SxPackingCargoCollection(guid'" + PackingId + "')")
                                        as HttpWebRequest;
                request.CookieContainer = bpmCookieContainer;
                // Для изменения записи используется метод PUT.
                request.Method = "PUT";
                request.Accept = "application/atom+xml";
                request.ContentType = "application/atom+xml;type=entry";

                // Create XML document.
                XmlDocument XMLdocument = new XmlDocument();

                // Create entry element.
                XmlElement entryElement = XMLdocument.CreateElement("entry", atom);

                // Create content element.
                XmlElement contentElement = XMLdocument.CreateElement("content", atom);
                // Create attributes.
                XmlAttribute typeAttr = XMLdocument.CreateAttribute("type");
                typeAttr.InnerText = "application/xml";
                contentElement.Attributes.Append(typeAttr);

                // Create content element.
                XmlElement propertiesElement = XMLdocument.CreateElement("properties", dsmd);                

                //  Флаг для последнего элемента после которого будет скрипт в BPM
                // Create elements.
                XmlElement needCalc = XMLdocument.CreateElement("SxNeedSetNumber", ds);
                needCalc.InnerText = "true";
                propertiesElement.AppendChild(needCalc);

                contentElement.AppendChild(propertiesElement);
                entryElement.AppendChild(contentElement);

                IAsyncResult getRequestStream = request.BeginGetRequestStream(null, null);
                var writer = new StreamWriter(request.EndGetRequestStream(getRequestStream));
                using (var writer1 = XmlWriter.Create(writer))
                {
                    entryElement.WriteTo(writer1);
                    writer1.Close();
                }
                writer.Close();
                request.BeginGetResponse(OnAsyncCallback, request);
                Cleverence.Log.Write("Отправил запрос на пересчет фасовки");
            }
        }
        private object DistributionPackages(Guid request, float weight, float volume, Guid loading, Guid unloading, Guid route, string nomenclature, int countRequest, SqlConnection dbConnection) {
            try {
                Cleverence.Log.Write("Начинаю распределение упаковок по отрезкам");
                if(dbConnection.State != System.Data.ConnectionState.Open)
                {
                    dbConnection.Open();
                }
                //  Получаем дополнительные данные
                var loadingAddress = "select SxWarehouseAddress from SxWarehouses where Id='"+loading+"'";
                using (SqlCommand Loading = new SqlCommand(loadingAddress, dbConnection))
                {
                    using (SqlDataReader loadingReader = Loading.ExecuteReader())
                    {
                        while (loadingReader.Read())
                        {
                            loadingAddress = loadingReader.GetValue(0).ToString();
                        }
                    }
                }
                var unloadingAddress = "select SxWarehouseAddress from SxWarehouses where Id=(select SxWarehouseId from SxWarehouseListInRoute where Id='"+unloading+"')";
                using (SqlCommand Unloading = new SqlCommand(unloadingAddress, dbConnection))
                {
                    using (SqlDataReader unloadingReader = Unloading.ExecuteReader())
                    {
                        while (unloadingReader.Read())
                        {
                            unloadingAddress = unloadingReader.GetValue(0).ToString();
                        }
                    }
                }
                var vehicle = "select SxVehicleId from SxRoute where Id='"+route+"'";
                using (SqlCommand Vehicle = new SqlCommand(vehicle, dbConnection))
                {
                    using (SqlDataReader vehicleReader = Vehicle.ExecuteReader())
                    {
                        while (vehicleReader.Read())
                        {
                            vehicle = vehicleReader.GetValue(0).ToString();
                        }
                    }
                }
                vehicle = vehicle == "" ? "null" : "'"+vehicle+"'";
                var packing = "select TOP 1 SxPackingKindId from SxPackingCargo where SxRequestId='"+request+"'";
                using (SqlCommand Packing = new SqlCommand(packing, dbConnection))
                {
                    using (SqlDataReader packingReader = Packing.ExecuteReader())
                    {
                        while (packingReader.Read())
                        {
                            packing = packingReader.GetValue(0).ToString();
                        }
                    }
                }
                packing = packing == "" ? "null" : "'" + packing + "'";

                int loadingRequestNumber = 0;
                int unloadingRequestNumber = 0;

                Guid requestLoading = Guid.Empty;
                if (loading != Guid.Empty && request != Guid.Empty && route != Guid.Empty) {
                    //  Ищем заявку в отрезке загрузки
                    var requestInSegment = "select r.Id, r.SxWeight, r.SxVolume, s.SxNumber, s.Id, r.SxCountRequest " +
                                            " from SxRequestVehicleRoute r join SxSegmentInRoute s " +
                                            " on r.SxNSegmentId = s.Id " +
                                            " where s.SxRouteId = '" + route.ToString() + "' " +
                                            " and s.SxWarehouseStartId = '" + loading.ToString() + "'" +
                                            " and r.SxRequestId = '" +request.ToString() + "'";

                    Guid segment = Guid.Empty;
                    int countRequestLoading = 0;
                    float requestWeight = 0;
                    float requestVolume = 0;
                    using (SqlCommand Request = new SqlCommand(requestInSegment, dbConnection))
                    {
                        using (SqlDataReader requestSegment = Request.ExecuteReader())
                        {
                            while (requestSegment.Read())
                            {
                                requestLoading = new Guid(requestSegment.GetValue(0).ToString());
                                requestWeight += float.Parse(requestSegment.GetValue(1).ToString());
                                requestVolume += float.Parse(requestSegment.GetValue(2).ToString());
                                loadingRequestNumber = int.Parse(requestSegment.GetValue(3).ToString());
                                segment = new Guid(requestSegment.GetValue(4).ToString());
                                countRequestLoading += int.Parse(requestSegment.GetValue(5).ToString());
                            }
                        }
                    }

                    requestWeight = requestWeight + weight;
                    requestVolume = requestVolume + volume;
                    countRequestLoading = countRequestLoading + countRequest;

                    if (requestLoading != Guid.Empty)
                    {
                        //  Прибавляем в найденную заявку вес и объем
                        var updateRequestInSegment = new SqlCommand("update SxRequestVehicleRoute " +
                                                                    " set SxWeight= " + requestWeight.ToString().Replace(",", ".") + ", SxVolume= " + requestVolume.ToString().Replace(",", ".") + ", "+
                                                                    " SxloadingId='"+loading+ "', SxloadingAddress='" + loadingAddress + "', SxCountRequest=" + countRequestLoading + ", SxVehicleInRouteId=" + vehicle +
                                                                    ", SxPackingKindId="+packing +
                                                                    " where Id = '" + requestLoading.ToString() + "'", dbConnection);
                        updateRequestInSegment.ExecuteNonQuery();
                    }
                    else
                    {
                        // Ищем отрезок загрузки
                        var SegmentLoading = "select Id, SxNumber from SxSegmentInRoute where SxRouteId='"+route.ToString()+ "' and SxWarehouseStartId='" + loading.ToString() + "'";
                        
                        segment = Guid.Empty;
                        using (SqlCommand Segment = new SqlCommand(SegmentLoading, dbConnection))
                        {
                            using (SqlDataReader requestSegment = Segment.ExecuteReader())
                            {
                                while (requestSegment.Read())
                                {
                                    segment = new Guid(requestSegment.GetValue(0).ToString());
                                    loadingRequestNumber = int.Parse(requestSegment.GetValue(1).ToString());
                                }
                            }
                        }
                        if (segment != Guid.Empty)
                        {
                            requestLoading = Guid.NewGuid();
                            //  Создаем запись с заявкой
                            var insertRequestInSegment = new SqlCommand(" insert into SxRequestVehicleRoute " +
                                                                        " (Id, SxNSegmentId, SxWeight, SxVolume, SxRequestId, SxloadingId, SxloadingAddress, SxNomenclatureId, SxCountRequest, SxVehicleInRouteId, SxPackingKindId) " +
                                                                        " values('"+ requestLoading.ToString() +"', '" + segment.ToString() + "', " + weight.ToString().Replace(",", ".") + ", " + volume.ToString().Replace(",", ".") + ", "+
                                                                        " '" + request.ToString() + "', '" + loading.ToString() + "', '" + loadingAddress + "', '" + nomenclature+"', "+countRequest+", "+vehicle+", "+
                                                                        packing+")", dbConnection);
                            insertRequestInSegment.ExecuteNonQuery();
                        }
                        else
                        {
                            Cleverence.Log.Write("Отрезок загрузки не найден!");
                            return true;
                        }
                    }
                }

                if (unloading != Guid.Empty && request != Guid.Empty && route != Guid.Empty)
                {
                    //  Ищем заявку в отрезке выгрузки
                    var requestInSegment = "select r.Id, r.SxWeight, r.SxVolume, s.SxNumber, s.Id, r.SxCountRequest " +
                                            " from SxRequestVehicleRoute r join SxSegmentInRoute s " +
                                            " on r.SxNSegmentId = s.Id " +
                                            " where s.SxRouteId = '" + route.ToString() + "' " +
                                            " and s.SxWarehouseEndId = (select SxWarehouseId from SxWarehouseListInRoute where Id='" + unloading.ToString() + "')" +
                                            " and r.SxRequestId = '" + request.ToString() + "'";

                    Guid requestUnloading = Guid.Empty;
                    Guid segment = Guid.Empty;
                    float requestWeight = 0;
                    float requestVolume = 0;
                    int countRequestUnloading = 0;
                    using (SqlCommand Request = new SqlCommand(requestInSegment, dbConnection))
                    {
                        using (SqlDataReader requestSegment = Request.ExecuteReader())
                        {
                            while (requestSegment.Read())
                            {
                                requestUnloading = new Guid(requestSegment.GetValue(0).ToString());
                                requestWeight += float.Parse(requestSegment.GetValue(1).ToString());
                                requestVolume += float.Parse(requestSegment.GetValue(2).ToString());
                                unloadingRequestNumber = int.Parse(requestSegment.GetValue(3).ToString());
                                segment = new Guid(requestSegment.GetValue(4).ToString());
                                countRequestUnloading = int.Parse(requestSegment.GetValue(5).ToString());
                            }
                        }
                    }

                    requestWeight = requestWeight + weight;
                    requestVolume = requestVolume + volume;
                    countRequestUnloading = countRequestUnloading + countRequest;

                    if (requestUnloading != Guid.Empty && requestUnloading == requestLoading)
                    {
                        //  Если это тот же отрезок что и для загрузки - просто проставляем выгрузку, без пересчета объема, веса и количества
                        var updateRequestInSegment = new SqlCommand("update SxRequestVehicleRoute " +
                                                                    " set SxunloadingId='" + unloading.ToString() + "', SxunloadingAddress='" + unloadingAddress + "'"+
                                                                    " where Id = '" + requestUnloading.ToString() + "'", dbConnection);
                        updateRequestInSegment.ExecuteNonQuery();
                    }else if (requestUnloading != Guid.Empty)
                    {
                        //  Прибавляем в найденную заявку вес и объем
                        var updateRequestInSegment = new SqlCommand("update SxRequestVehicleRoute " +
                                                                    " set SxWeight= " + requestWeight.ToString().Replace(",", ".") + ", SxVolume= " + requestVolume.ToString().Replace(",", ".") + ", "+
                                                                    " SxunloadingId='"+unloading.ToString()+ "', SxunloadingAddress='"+unloadingAddress+"', SxCountRequest=" + countRequestUnloading + ", SxVehicleInRouteId=" + vehicle +
                                                                    ", SxPackingKindId=" + packing +
                                                                    " where Id = '" + requestUnloading.ToString() + "'", dbConnection);
                        updateRequestInSegment.ExecuteNonQuery();
                    }
                    else
                    {
                        // Ищем отрезок выгрузки
                        var SegmentUnloading = "select Id, SxNumber from SxSegmentInRoute where SxRouteId='" + route.ToString() + "' "+
                                                " and SxWarehouseEndId= (select SxWarehouseId from SxWarehouseListInRoute where Id='"+unloading.ToString()+"')";

                        segment = Guid.Empty;
                        using (SqlCommand Segment = new SqlCommand(SegmentUnloading, dbConnection))
                        {
                            using (SqlDataReader requestSegment = Segment.ExecuteReader())
                            {
                                while (requestSegment.Read())
                                {
                                    segment = new Guid(requestSegment.GetValue(0).ToString());
                                    unloadingRequestNumber = int.Parse(requestSegment.GetValue(1).ToString());
                                }
                            }
                        }
                        if (segment != Guid.Empty)
                        {
                            //  Создаем запись с заявкой
                            var insertRequestInSegment = new SqlCommand(" insert into SxRequestVehicleRoute " +
                                                                        " (SxNSegmentId, SxWeight, SxVolume, SxRequestId, SxunloadingId, SxunloadingAddress, SxNomenclatureId, SxCountRequest, SxVehicleInRouteId, SxPackingKindId) " +
                                                                        " values('" + segment.ToString() + "', " + weight.ToString().Replace(",", ".") + ", " + volume.ToString().Replace(",", ".") + ", "+
                                                                        " '" + request.ToString() + "', '" + unloading.ToString() + "', '"+unloadingAddress+"', '"+nomenclature+"', "+countRequest+", "+vehicle+", "+packing+")"
                                                                        , dbConnection);
                            insertRequestInSegment.ExecuteNonQuery();
                        }
                        else
                        {
                            Cleverence.Log.Write("Отрезок выгрузки не найден!");
                            return true;
                        }
                    }
                }

                for (var i = loadingRequestNumber + 1; i < unloadingRequestNumber; i++) {

                    //  Ищем заявки в промежуточных отрезках
                    var requestInSegment = "select r.Id, r.SxWeight, r.SxVolume, s.Id, r.SxCountRequest from SxRequestVehicleRoute r join SxSegmentInRoute s " +
                                            " on r.SxNSegmentId = s.Id " +
                                            " where s.SxNumber = " + i + " and s.SxRouteId = '" + route.ToString() + "'" +
                                            " and r.SxRequestId = '" + request.ToString() + "'";

                    Guid requestMedium = Guid.Empty;
                    Guid segment = Guid.Empty;
                    float requestWeight = 0;
                    float requestVolume = 0;
                    int countRequestInSegment = 0;
                    using (SqlCommand Request = new SqlCommand(requestInSegment, dbConnection))
                    {
                        using (SqlDataReader requestSegment = Request.ExecuteReader())
                        {
                            while (requestSegment.Read())
                            {
                                requestMedium = new Guid(requestSegment.GetValue(0).ToString());
                                requestWeight += float.Parse(requestSegment.GetValue(1).ToString());
                                requestVolume += float.Parse(requestSegment.GetValue(2).ToString());
                                segment = new Guid(requestSegment.GetValue(3).ToString());
                                countRequestInSegment += int.Parse(requestSegment.GetValue(4).ToString());
                            }
                        }
                    }

                    requestWeight = requestWeight + weight;
                    requestVolume = requestVolume + volume;
                    countRequestInSegment = countRequestInSegment + countRequest;

                    if (requestMedium != Guid.Empty)
                    {
                        //  Прибавляем в найденную заявку вес и объем
                        var updateRequestInSegment = new SqlCommand("update SxRequestVehicleRoute " +
                                                                    " set SxWeight = " + requestWeight.ToString().Replace(",", ".") + ", SxVolume = " + requestVolume.ToString().Replace(",", ".") +
                                                                    ", SxCountRequest="+countRequestInSegment+ ", SxVehicleInRouteId="+vehicle+", "+
                                                                    " SxPackingKindId="+packing+
                                                                    " where Id = '" + requestMedium.ToString() + "'", dbConnection);
                        updateRequestInSegment.ExecuteNonQuery();
                    }
                    else
                    {
                        // Ищем отрезок выгрузки
                        var SegmentMedium = "select Id from SxSegmentInRoute where SxRouteId='" + route.ToString() + "' and SxNumber="+i;

                        segment = Guid.Empty;
                        using (SqlCommand Segment = new SqlCommand(SegmentMedium, dbConnection))
                        {
                            using (SqlDataReader requestSegment = Segment.ExecuteReader())
                            {
                                while (requestSegment.Read())
                                {
                                    segment = new Guid(requestSegment.GetValue(0).ToString());
                                }
                            }
                        }
                        if (segment != Guid.Empty)
                        {
                            //  Создаем запись с заявкой
                            var insertRequestInMediumSegment = new SqlCommand("insert into SxRequestVehicleRoute " +
                                                                            " (SxNSegmentId, SxWeight, SxVolume, SxRequestId, SxNomenclatureId, SxCountRequest, SxVehicleInRouteId, SxPackingKindId) " +
                                                                            " values('" + segment.ToString() + "', " + weight.ToString().Replace(",", ".") + ", " + volume.ToString().Replace(",", ".") + ", "+
                                                                            " '" + request.ToString() + "', '"+nomenclature+"', "+ countRequest+", "+vehicle+", "+packing+")", dbConnection);
                            insertRequestInMediumSegment.ExecuteNonQuery();
                        }
                        else
                        {
                            Cleverence.Log.Write("Промежуточный отрезок не найден!");
                            return true;
                        }
                    }

                }
                Cleverence.Log.Write("Упаковки распределены по отрезкам");
            }
            catch (Exception ex)
            {
                Cleverence.Log.Write("Ошибка при распределении упаковок по отрезкам: ", ex, Cleverence.Log.LogType.Error);
            }
            return true;
        }
        private object insertShippingSheet(List<List<string>> ShippingCargo, string documentId, string documentWarehouseId, string documentDescription, string unloading) {
            float weight = 0;
            float volume = 0;
            int i = 0;
            //  Добавляем на отгрузочный лист грузы 
            foreach (var cargo in ShippingCargo)
            {
                string SxNomenclatureId = cargo[4] != "" ? "'" + cargo[4] + "'" : "null";
                weight += float.Parse(cargo[2]);
                volume += float.Parse(cargo[3]);
                var SxWeight = cargo[2] != "" ? cargo[2].Replace(",", ".") : "0";
                var SxVolume = cargo[3] != "" ? cargo[3].Replace(",", ".") : "0";
                var SxUnloadingId = unloading != "" ? "'"+unloading+ "'" : "null";

                var insertShippingCargo = new SqlCommand(" insert into SxShippingSheetInRoute " +
                " (SxRouteId, SxLoadingId, SxUploaded, SxReserved, SxNumberRequestId, SxNeedCalc, SxNomenclatureId, SxMarkingGoods, SxMarkingGoodsLookupId, SxWeight, SxVolume, SxUnloadingId) " +
                "values('" + documentId + "', '" + documentWarehouseId + "', 1, 0, '" + documentDescription + "', 0, " + SxNomenclatureId + ", '" + cargo[0] + "', '" + cargo[1] + "', " + SxWeight + ", " + SxVolume + ", " + SxUnloadingId + ")", dbConnection);
                i += insertShippingCargo.ExecuteNonQuery();
            }

            Cleverence.Log.Write("Добавил на отгрузочный лист " + i + " упаковок");
            if(unloading == "")
            {
                unloading = Guid.Empty.ToString();
            }
            DistributionPackages(new Guid(documentDescription), weight, volume, new Guid(documentWarehouseId), new Guid(unloading), new Guid(documentId), ShippingCargo[0][4], i, dbConnection);

            return true;
        }
        private int updatePackingCargo(string documentId, int Quantity, string PackingSelect, string documentDescription, string documentWarehouseId) {
            if (dbConnection.State != System.Data.ConnectionState.Open)
            {
                dbConnection.Open();
            }
            int countCargo = 0;

            var getPackingIdString = "select TOP(" + Quantity + ") Id from SxPackingCargo " + PackingSelect;

            var PackingId = new List<string>();
            using (SqlCommand Packing = new SqlCommand(getPackingIdString, dbConnection))
            {
                using (SqlDataReader id = Packing.ExecuteReader())
                {
                    while (id.Read())
                    {
                        PackingId.Add(id.GetValue(0).ToString());
                    }
                }
            }
            if (PackingId.Count <= 0)
            {
                return 0;
            }
                var updatePackingCargo = new SqlCommand(" update SxPackingCargo " +
                                                                    " set SxCurrentWarehouseId=null, " +
                                                                    " SxCurrentShelfId=null, " +
                                                                    " SxReserved=0, SxEntrepot=0," +
                                                                    " SxCurrentRouteId='" + documentId + "', " +
                                                                    " SxStateId=(select Id from SxRequestState where Id='2D87000B-30B9-46D5-8156-7706F50C6B74' OR Name='В пути'), " +
                                                                    " SxNeedSetNumber=0 " +
                                                                    " where Id in (select TOP(" + (PackingId.Count - 1) + ") Id from SxPackingCargo " + PackingSelect+")", dbConnection);
                countCargo += updatePackingCargo.ExecuteNonQuery();

            Cleverence.Log.Write("Обновил информацию на отгрузочном листе. Кол-во: "+countCargo);     

            // Создание экземпляра запроса для изменения контакта с заданным идентификатором.
            var request = HttpWebRequest.Create(serverUri + "SxPackingCargoCollection(guid'" + PackingId[(PackingId.Count - 1)] + "')")
                                        as HttpWebRequest;
            request.CookieContainer = bpmCookieContainer;
            // Для изменения записи используется метод PUT.
            request.Method = "PUT";
            request.Accept = "application/atom+xml";
            request.ContentType = "application/atom+xml;type=entry";

            // Create XML document.
            XmlDocument XMLdocumentPackingCargo2 = new XmlDocument();


            // Create entry element.
            XmlElement entryElementPackingCargo2 = XMLdocumentPackingCargo2.CreateElement("entry", atom);

            // Create content element.
            XmlElement contentElementPackingCargo2 = XMLdocumentPackingCargo2.CreateElement("content", atom);
            // Create attributes.
            XmlAttribute typeAttrPackingCargo2 = XMLdocumentPackingCargo2.CreateAttribute("type");
            typeAttrPackingCargo2.InnerText = "application/xml";
            contentElementPackingCargo2.Attributes.Append(typeAttrPackingCargo2);

            // Create content element.
            XmlElement propertiesElementPackingCargo2 = XMLdocumentPackingCargo2.CreateElement("properties", dsmd);


            // Create elements.
            XmlElement CurrentWarehouse = XMLdocumentPackingCargo2.CreateElement("SxCurrentWarehouseId", ds);
            // Create attributes.
            XmlAttribute warehouseAttr = XMLdocumentPackingCargo2.CreateAttribute("p1", "null", dsmd);
            warehouseAttr.InnerText = "true";
            CurrentWarehouse.Attributes.Append(warehouseAttr);
            XmlAttribute warehouseAttr1 = XMLdocumentPackingCargo2.CreateAttribute("xmlns");
            warehouseAttr1.InnerText = ds;
            CurrentWarehouse.Attributes.Append(warehouseAttr1);
            propertiesElementPackingCargo2.AppendChild(CurrentWarehouse);

            // Create elements.
            XmlElement CurrentShelf = XMLdocumentPackingCargo2.CreateElement("SxCurrentShelfId", ds);
            // Create attributes.
            XmlAttribute ShelfAttr = XMLdocumentPackingCargo2.CreateAttribute("p2", "null", dsmd);
            ShelfAttr.InnerText = "true";
            CurrentShelf.Attributes.Append(ShelfAttr);
            XmlAttribute ShelfAttr1 = XMLdocumentPackingCargo2.CreateAttribute("xmlns");
            ShelfAttr1.InnerText = ds;
            CurrentShelf.Attributes.Append(ShelfAttr1);
            propertiesElementPackingCargo2.AppendChild(CurrentShelf);

            // Create elements.
            XmlElement ReservedPackingCargo2 = XMLdocumentPackingCargo2.CreateElement("SxReserved", ds);
            // Create attributes.
            ReservedPackingCargo2.InnerText = "false";
            propertiesElementPackingCargo2.AppendChild(ReservedPackingCargo2);
            // Create elements.
            XmlElement Entrepot = XMLdocumentPackingCargo2.CreateElement("SxEntrepot", ds);
            // Create attributes.
            Entrepot.InnerText = "false";
            propertiesElementPackingCargo2.AppendChild(Entrepot);
            // Create elements.
            XmlElement CurrentRoute = XMLdocumentPackingCargo2.CreateElement("SxCurrentRouteId", ds);
            // Create attributes.
            CurrentRoute.InnerText = documentId;
            propertiesElementPackingCargo2.AppendChild(CurrentRoute);

            Cleverence.Log.Write("Обновляю последнюю фасовку. Маршрут: " + documentId);

            // Create elements.
            XmlElement StatePackingCargo2 = XMLdocumentPackingCargo2.CreateElement("SxStateId", ds);
            // Create attributes.
            StatePackingCargo2.InnerText = "2D87000B-30B9-46D5-8156-7706F50C6B74";
            propertiesElementPackingCargo2.AppendChild(StatePackingCargo2);

            // Флаг для последнего элемента после которого будет скрипт в BPM
            // Create elements.
            XmlElement needCalcPackingCargo2 = XMLdocumentPackingCargo2.CreateElement("SxNeedSetNumber", ds);
            needCalcPackingCargo2.InnerText = "true";
            propertiesElementPackingCargo2.AppendChild(needCalcPackingCargo2);

            contentElementPackingCargo2.AppendChild(propertiesElementPackingCargo2);
            entryElementPackingCargo2.AppendChild(contentElementPackingCargo2);

            // Запись сообщения xml в поток запроса.
            /*using (var writer1 = XmlWriter.Create(request.GetRequestStream()))
            {
                entryElementPackingCargo2.WriteTo(writer1);
            }

            Stream dataStream = request.GetRequestStream();
            dataStream.Close();

            // Получение ответа от сервиса о результате выполнения операции.
            using (WebResponse response = request.GetResponse())
            {
                countCargo++;
            }  */            

            IAsyncResult getRequestStream = request.BeginGetRequestStream(null, null);
            var writer = new StreamWriter(request.EndGetRequestStream(getRequestStream));            
            using (var writer1 = XmlWriter.Create(writer))
            {
                entryElementPackingCargo2.WriteTo(writer1);
                writer1.Close();
            }
            writer.Close();            
            request.BeginGetResponse(OnAsyncCallback, request);
            Cleverence.Log.Write("Отправил запрос на пересчет фасовки");
            
            return countCargo+1;
        }
        private static void OnAsyncCallback(IAsyncResult asyncResult)
        {
            try
            {
                var httpWebRequest = (HttpWebRequest)asyncResult.AsyncState;
                WebResponse response = httpWebRequest.EndGetResponse(asyncResult);
                //response.Close();
                Cleverence.Log.Write("Фасовка пересчиталась успешно");
            }
            catch (Exception ex)
            {
                Cleverence.Log.Write("Фасовка не пересчиталась", ex, Cleverence.Log.LogType.Error);
            }
        }
        private object Loading(Cleverence.Warehouse.Document document, int Quantity, bool lastRequest)
        {
            if (dbConnection.State != System.Data.ConnectionState.Open)
            {
                dbConnection.Open();
            }
            int countCargo = 0;
            //  1. Груз зарезервированный в этом маршруте            
            //  Удаляем с отгрузочного листа зарезервированный груз
            var deleteShippingCargo = new SqlCommand("delete SxShippingSheetInRoute where SxMarkingGoodsLookupId in " +
                                      " (select TOP(" + Quantity + ") Id from SxPackingCargo " +
                                      " where SxRequestId='" + document.Description + "' " +
                                      " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                      " AND SxReserved=1 " +
                                      " AND SxCurrentRouteId='" + document.Id + "' ORDER BY CreatedOn ASC)"
                                      , dbConnection);
            deleteShippingCargo.ExecuteNonQuery();

            //  Добавляем на отгрузлочный лист новый груз
            //  Берём информацию о грузе
            var getShippingSheetSqlString = " select TOP(" + Quantity + ") SxNumber, Id, SxWeight, SxVolume, SxNomenclatureId from SxPackingCargo " +
                                                         " where SxRequestId='" + document.Description + "' " +
                                                         " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                                         " AND SxReserved=1 " +
                                                         " AND SxCurrentRouteId='" + document.Id + "' ORDER BY CreatedOn ASC";
            List<List<string>> ShippingCargo = new List<List<string>>(); //инициализация
            int i = 0;
            using (SqlCommand PackingCargo = new SqlCommand(getShippingSheetSqlString, dbConnection))
            {
                using (SqlDataReader cargo = PackingCargo.ExecuteReader())
                {
                    while (cargo.Read())
                    {
                        ShippingCargo.Add(new List<string>());//добавление новой строки
                        ShippingCargo[i].Add(cargo.GetValue(0).ToString());
                        ShippingCargo[i].Add(cargo.GetValue(1).ToString());
                        ShippingCargo[i].Add(cargo.GetValue(2).ToString());
                        ShippingCargo[i].Add(cargo.GetValue(3).ToString());
                        ShippingCargo[i].Add(cargo.GetValue(4).ToString());
                        i++;
                    }
                }
            }

            var unloading = "";
            var getUnloadingIdString = "select Id from SxWarehouseListInRoute where SxRouteId='" + document.Id + "' " +
                                     "AND SxWarehouseId=(select SxDueWarehouseId from SxRequest where Id='" + document.Description + "' )";
            using (SqlCommand Packing = new SqlCommand(getUnloadingIdString, dbConnection))
            {
                using (SqlDataReader id = Packing.ExecuteReader())
                {
                    while (id.Read())
                    {
                        unloading = id.GetValue(0).ToString();
                    }
                }
            }

            if (ShippingCargo.Count > 0)
            {
                //  Добавляем упаковку на отгрузочный лист
                insertShippingSheet(ShippingCargo, document.Id, document.WarehouseId, document.Description, unloading);
            }

            //  Обновляем зарезервированные в этом маршруте грузы на детале Фасовка груза

            var PackingSelect = " where SxRequestId='" + document.Description + "' " +
                                " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                " AND SxReserved=1 " +
                                " AND SxCurrentRouteId='" + document.Id + "' ORDER BY CreatedOn ASC";
            
            countCargo += updatePackingCargo(document.Id, Quantity, PackingSelect, document.Description, document.WarehouseId);
                     

            Cleverence.Log.Write("Загрузил зарезервированный груз в этом маршруте в количестве " + countCargo + " шт. Осталось: " + (Quantity - countCargo) + " шт.");
            
            if (Quantity > countCargo)
            {
                //  2. Не зарезервированные грузы
                //  Добавляем на отгрузочный лист новый груз
                //  Берём информацию о грузе
                getShippingSheetSqlString = " select TOP(" + (Quantity-countCargo) + ") SxNumber, Id, SxWeight, SxVolume, SxNomenclatureId from SxPackingCargo " +
                                                             " where SxRequestId='" + document.Description + "' " +
                                                             " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                                             " AND SxReserved=0 ORDER BY CreatedOn ASC";
                ShippingCargo.Clear();
                i = 0;
                using (SqlCommand PackingCargo = new SqlCommand(getShippingSheetSqlString, dbConnection))
                {
                    using (SqlDataReader cargo = PackingCargo.ExecuteReader())
                    {
                        while (cargo.Read())
                        {
                            ShippingCargo.Add(new List<string>());//добавление новой строки
                            ShippingCargo[i].Add(cargo.GetValue(0).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(1).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(2).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(3).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(4).ToString());
                            i++;
                        }
                    }
                }

                unloading = "";
                getUnloadingIdString = "select Id from SxWarehouseListInRoute where SxRouteId='" + document.Id + "' " +
                                         "AND SxWarehouseId=(select SxDueWarehouseId from SxRequest where Id='" + document.Description + "' )";
                using (SqlCommand Packing = new SqlCommand(getUnloadingIdString, dbConnection))
                {
                    using (SqlDataReader id = Packing.ExecuteReader())
                    {
                        while (id.Read())
                        {
                            unloading = id.GetValue(0).ToString();
                        }
                    }
                }

                if (ShippingCargo.Count > 0)
                {
                    //  Добавляем упаковку на отгрузочный лист
                    insertShippingSheet(ShippingCargo, document.Id, document.WarehouseId, document.Description, unloading);
                }

                //  Обновляем не зарезервированные в этом маршруте грузы на детале Фасовка груза

                PackingSelect = " where SxRequestId='" + document.Description + "' " +
                                " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                " AND SxReserved=0 ORDER BY CreatedOn ASC";
                countCargo += updatePackingCargo(document.Id, (Quantity - countCargo), PackingSelect, document.Description, document.WarehouseId);
                
            }
            Cleverence.Log.Write("Загрузил не зарезервированный и зарезервированный груз в количестве " + countCargo + " шт. Осталось: " + (Quantity - countCargo) + " шт.");
            if (Quantity > countCargo)
            {
                // 3. Зарезервированные грузы в другом маршруте
                //  Удаляем с отгрузочного листа зарезервированный груз
                deleteShippingCargo = new SqlCommand("delete SxShippingSheetInRoute where SxMarkingGoodsLookupId in " +
                                      " (select TOP(" + (Quantity - countCargo) + ") Id from SxPackingCargo " +
                                      " where SxRequestId='" + document.Description + "' " +
                                      " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                      " AND SxReserved=1 " +
                                      " AND SxCurrentRouteId!='" + document.Id + "' ORDER BY CreatedOn ASC)", dbConnection);
                deleteShippingCargo.ExecuteNonQuery();

                //  Добавляем на отгрузлочный лист новый груз
                //  Берём информацию о грузе
                getShippingSheetSqlString = " select TOP(" + (Quantity - countCargo) + ") SxNumber, Id, SxWeight, SxVolume, SxNomenclatureId from SxPackingCargo " +
                                                             " where SxRequestId='" + document.Description + "' " +
                                                             " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                                             " AND SxReserved=1 " +
                                                             " AND SxCurrentRouteId!='" + document.Id + "' ORDER BY CreatedOn ASC";
                ShippingCargo.Clear();
                i = 0;
                using (SqlCommand PackingCargo = new SqlCommand(getShippingSheetSqlString, dbConnection))
                {
                    using (SqlDataReader cargo = PackingCargo.ExecuteReader())
                    {
                        while (cargo.Read())
                        {
                            ShippingCargo.Add(new List<string>());//добавление новой строки
                            ShippingCargo[i].Add(cargo.GetValue(0).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(1).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(2).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(3).ToString());
                            ShippingCargo[i].Add(cargo.GetValue(4).ToString());
                            i++;
                        }
                    }
                }

                unloading = "";
                getUnloadingIdString = "select Id from SxWarehouseListInRoute where SxRouteId='" + document.Id + "' " +
                                         "AND SxWarehouseId=(select SxDueWarehouseId from SxRequest where Id='" + document.Description + "' )";
                using (SqlCommand Packing = new SqlCommand(getUnloadingIdString, dbConnection))
                {
                    using (SqlDataReader id = Packing.ExecuteReader())
                    {
                        while (id.Read())
                        {
                            unloading = id.GetValue(0).ToString();
                        }
                    }
                }
                if(ShippingCargo.Count > 0)
                {
                    insertShippingSheet(ShippingCargo, document.Id, document.WarehouseId, document.Description, unloading);
                }


                //  Обновляем зарезервированные в этом маршруте грузы на детале Фасовка груза

                PackingSelect = " where SxRequestId='" + document.Description + "' " +
                                " AND SxCurrentWarehouseId='" + document.WarehouseId + "' " +
                                " AND SxReserved=1 " +
                                " AND SxCurrentRouteId!='" + document.Id + "' ORDER BY CreatedOn ASC";
                countCargo += updatePackingCargo(document.Id, (Quantity - countCargo), PackingSelect, document.Description, document.WarehouseId);
                              
            }
            Cleverence.Log.Write("Всего загрузил " + countCargo + " шт. Осталось: "+(Quantity - countCargo)+" шт.");
            if (Quantity > countCargo)
            {
                //  Инсерт в Заявка.НЕ КОРРЕКТНАЯ РАЗГРУЗКА/ПОГРУЗКА
                SqlCommand insertIncorrect = new SqlCommand(" insert into SxIncorrectLoadUnload (SxRequestId, SxWarehouseId, SxLoadingExceededBy) " +
                                                            " VALUES ('" + document.Description + "', '" + document.WarehouseId + "', " + (Quantity - countCargo) + ")", dbConnection);
                insertIncorrect.ExecuteNonQuery();

                Cleverence.Log.Write("Добавил на некорректную разгрузку/погрузку: " + (Quantity - countCargo) + " шт.");
            }

            return null;
        }
        private object LoadingToDoor(Cleverence.Warehouse.Document document, int Quantity, string requestNumber)
        {
            if (dbConnection.State != System.Data.ConnectionState.Open)
            {
                dbConnection.Open();
            }
            var getItineraryMarkingGoodsString = "select TOP(" + Quantity + ") SxMarkingGoodsId from SxItineraryToDoor "+ 
                                                " where SxNumberRequestId='" + document.Description + "' AND SxRouteId='" + document.Id + "'"+
                                                " AND SxUploaded!=1 ORDER BY CreatedOn ASC";

            var ItineraryMarkingGoods = new List<string>();
            using (SqlCommand Itinerary = new SqlCommand(getItineraryMarkingGoodsString, dbConnection))
            {
                using (SqlDataReader id = Itinerary.ExecuteReader())
                {
                    while (id.Read())
                    {
                        ItineraryMarkingGoods.Add(id.GetValue(0).ToString());
                    }
                }
            }
            var cargoCount = 0;
            foreach (var guid in ItineraryMarkingGoods)
            {

                // Создание экземпляра запроса для изменения контакта с заданным идентификатором.
                var request = HttpWebRequest.Create(serverUri + "SxCargoForwardingCollection(guid'" + guid + "')")
                                        as HttpWebRequest;
                request.CookieContainer = bpmCookieContainer;
                // Для изменения записи используется метод PUT.
                request.Method = "PUT";
                request.Accept = "application/atom+xml";
                request.ContentType = "application/atom+xml;type=entry";

                // Create XML document.
                XmlDocument XMLdocument = new XmlDocument();

                // Create entry element.
                XmlElement entryElement = XMLdocument.CreateElement("entry", atom);

                // Create content element.
                XmlElement contentElement = XMLdocument.CreateElement("content", atom);
                // Create attributes.
                XmlAttribute typeAttr = XMLdocument.CreateAttribute("type");
                typeAttr.InnerText = "application/xml";
                contentElement.Attributes.Append(typeAttr);

                // Create content element.
                XmlElement propertiesElement = XMLdocument.CreateElement("properties", dsmd);

                // Create elements.
                XmlElement State = XMLdocument.CreateElement("SxStateId", ds);
                // Create attributes.
                State.InnerText = "2D87000B-30B9-46D5-8156-7706F50C6B74";
                propertiesElement.AppendChild(State);

                // Create elements.
                XmlElement CurrentWarehouse = XMLdocument.CreateElement("SxCurrentWarehouseId", ds);
                // Create attributes.
                XmlAttribute warehouseAttr = XMLdocument.CreateAttribute("p1", "null", dsmd);
                warehouseAttr.InnerText = "true";
                CurrentWarehouse.Attributes.Append(warehouseAttr);
                XmlAttribute warehouseAttr1 = XMLdocument.CreateAttribute("xmlns");
                warehouseAttr1.InnerText = ds;
                CurrentWarehouse.Attributes.Append(warehouseAttr1);
                propertiesElement.AppendChild(CurrentWarehouse);

                // Create elements.
                XmlElement CurrentRoute = XMLdocument.CreateElement("SxCurrentRouteId", ds);
                // Create attributes.
                CurrentRoute.InnerText = document.Id;
                propertiesElement.AppendChild(CurrentRoute);

                Cleverence.Log.Write("Обновляю последнюю фасовку. Маршрут: " + document.Id);

                contentElement.AppendChild(propertiesElement);
                entryElement.AppendChild(contentElement);

                // Запись сообщения xml в поток запроса.
                using (var writer = XmlWriter.Create(request.GetRequestStream()))
                {
                    entryElement.WriteTo(writer);
                    writer.Close();
                }

                request.GetRequestStream().Close();

                // Получение ответа от сервиса о результате выполнения операции.
                try
                {
                    using (WebResponse response = request.GetResponse())
                    {
                        cargoCount++;
                    }
                }
                catch (Exception ex)
                {
                    Cleverence.Log.Write("Ошибка загрузки в маршрут экспедирования ", ex, Cleverence.Log.LogType.Error);
                }
            }

            /*SqlCommand updateCargoForwarding = new SqlCommand("update SxCargoForwarding "+
                                                            " set SxStateId=(select Id from SxRequestState where Name='В пути' OR Id='2D87000B-30B9-46D5-8156-7706F50C6B74'), "+
                                                            " SxCurrentWarehouseId=null, SxCurrentRouteId='"+document.Id+"' "+
                                                            " where Id in (select TOP(" + Quantity + ") SxMarkingGoodsId from SxItineraryToDoor where SxNumberRequestId='" + document.Description + "' AND SxRouteId='" + document.Id + "'  ORDER BY CreatedOn ASC)", dbConnection);
            int cargoCount = updateCargoForwarding.ExecuteNonQuery();*/

            var getItineraryIdString = "select TOP(" + Quantity + ") Id from SxItineraryToDoor where SxNumberRequestId='" + document.Description + "'"+
                                        " AND SxRouteId='" + document.Id + "' AND SxUploaded !=1 ORDER BY CreatedOn ASC";

            var ItineraryId = new List<string>();
            using (SqlCommand Itinerary = new SqlCommand(getItineraryIdString, dbConnection))
            {
                using (SqlDataReader id = Itinerary.ExecuteReader())
                {
                    while (id.Read())
                    {
                        ItineraryId.Add(id.GetValue(0).ToString());
                    }
                }
            }
            //FormsAuthRequest();
            foreach (var guid in ItineraryId)
            {

                // Создание экземпляра запроса для изменения контакта с заданным идентификатором.
                var request = HttpWebRequest.Create(serverUri + "SxItineraryToDoorCollection(guid'" + guid + "')")
                                        as HttpWebRequest;
                request.CookieContainer = bpmCookieContainer;
                // Для изменения записи используется метод PUT.
                request.Method = "PUT";
                request.Accept = "application/atom+xml";
                request.ContentType = "application/atom+xml;type=entry";

                // Create XML document.
                XmlDocument XMLdocument = new XmlDocument();

                // Create entry element.
                XmlElement entryElement = XMLdocument.CreateElement("entry", atom);

                // Create content element.
                XmlElement contentElement = XMLdocument.CreateElement("content", atom);
                // Create attributes.
                XmlAttribute typeAttr = XMLdocument.CreateAttribute("type");
                typeAttr.InnerText = "application/xml";
                contentElement.Attributes.Append(typeAttr);

                // Create content element.
                XmlElement propertiesElement = XMLdocument.CreateElement("properties", dsmd);

                // Create elements.
                XmlElement State = XMLdocument.CreateElement("SxUploaded", ds);
                // Create attributes.
                State.InnerText = "true";
                propertiesElement.AppendChild(State);

                contentElement.AppendChild(propertiesElement);
                entryElement.AppendChild(contentElement);

                // Запись сообщения xml в поток запроса.
                using (var writer = XmlWriter.Create(request.GetRequestStream()))
                {
                    entryElement.WriteTo(writer);
                    writer.Close();
                }

                request.GetRequestStream().Close();

                // Получение ответа от сервиса о результате выполнения операции.
                try
                {
                    using (WebResponse response = request.GetResponse())
                    {
                        //countPackingCargo++;
                    }
                }
                catch (Exception ex)
                {
                    Cleverence.Log.Write("Ошибка загрузки в маршрут экспедирования ", ex, Cleverence.Log.LogType.Error);
                }
            }

            /*SqlCommand updateItinerary = new SqlCommand("update SxItineraryToDoor set SxUploaded=1 "+
                                                        "where Id in (select TOP(" + Quantity + ") Id from SxItineraryToDoor where SxNumberRequestId='" + document.Description + "' AND SxRouteId='" + document.Id + "' ORDER BY CreatedOn ASC)", dbConnection);
            updateItinerary.ExecuteNonQuery();*/


            if (Quantity > cargoCount)
            {
                SqlCommand insertRouteNotes = new SqlCommand("update SxRoute set SxNotes=" +
                                                              "(select SxNotes from SxRoute where Id='" + document.Id + "')+ " +
                                                              "'<div>Заявка № <a href=\"/0/Nui/ViewModule.aspx#SectionModuleV2/SxRequestSection/SxRequestPage/edit/" + document.Description + "\">" + requestNumber + "</a> была загружена в количестве " + (Quantity - cargoCount) + "шт не по маршруту.' " +
                                                              " where Id='" + document.Id + "\n'", dbConnection);
                insertRouteNotes.ExecuteNonQuery();
            }
            return null;
        }

        private object checkUnloading(string unloadingId, string unloadingOld, string routeId, string requestId, int Count, float Weight, float Volume) {
            try {
                if (dbConnection.State != System.Data.ConnectionState.Open)
                {
                    dbConnection.Open();
                }
                if (unloadingOld != unloadingId)
                {
                    int segmentUnloadingNumber = 0;
                    var segmentUnloading = "select SxNumber from SxSegmentInRoute where SxRouteId='" + routeId + "' " +
                                           " AND SxWarehouseEndId = (select SxWarehouseId from SxWarehouseListInRoute where Id = '" + unloadingId + "')";

                    using (SqlCommand Unloading = new SqlCommand(segmentUnloading, dbConnection))
                    {
                        using (SqlDataReader unloading = Unloading.ExecuteReader())
                        {
                            while (unloading.Read())
                            {
                                segmentUnloadingNumber = int.Parse(unloading.GetValue(0).ToString());
                            }
                        }
                    }
                    if (!(segmentUnloadingNumber > 0)) {
                        var segmentLoading = "select SxNumber from SxSegmentInRoute where SxRouteId='" + routeId + "' " +
                                           " AND SxWarehouseStartId = (select SxWarehouseId from SxWarehouseListInRoute where Id = '" + unloadingId + "')";

                        using (SqlCommand Unloading = new SqlCommand(segmentLoading, dbConnection))
                        {
                            using (SqlDataReader unloading = Unloading.ExecuteReader())
                            {
                                while (unloading.Read())
                                {
                                    segmentUnloadingNumber = int.Parse(unloading.GetValue(0).ToString());
                                }
                            }
                        }

                        if (!(segmentUnloadingNumber > 0))
                        { return true; }

                        var updateData = new List<List<string>>(); //инициализация
                        var segmentAfterUnloading = "select s.Id, r.SxWeight, r.SxVolume, r.SxCountRequest from SxSegmentInRoute s " +
                                                    " JOIN SxRequestVehicleRoute r on s.Id = r.SxNSegmentId " +
                                                    " where s.SxRouteId = '" + routeId + "' AND r.SxRequestId = '" + requestId + "'" +
                                                    " AND s.SxNumber >= " + segmentUnloadingNumber;
                        int i = 0;
                        using (SqlCommand Segment = new SqlCommand(segmentAfterUnloading, dbConnection))
                        {
                            using (SqlDataReader segment = Segment.ExecuteReader())
                            {
                                while (segment.Read())
                                {
                                    updateData.Add(new List<string>());//добавление новой строки
                                    updateData[i].Add(segment.GetValue(0).ToString());
                                    updateData[i].Add(segment.GetValue(1).ToString());
                                    updateData[i].Add(segment.GetValue(2).ToString());
                                    updateData[i].Add(segment.GetValue(3).ToString());
                                    i++;
                                }
                            }
                        }

                        foreach (var data in updateData)
                        {
                            float newWeight = float.Parse(data[1]) - Weight;
                            float newVolume = float.Parse(data[2]) - Volume;
                            int newCount = int.Parse(data[3]) - Count;
                            if (newCount == 0)
                            {
                                SqlCommand updateRequestVehicle = new SqlCommand("delete SxRequestVehicleRoute where SxNSegmentId = '" + data[0] + "' AND SxRequestId='" + requestId + "'", dbConnection);
                                updateRequestVehicle.ExecuteNonQuery();
                            } else{
                                SqlCommand updateRequestVehicle = new SqlCommand("update SxRequestVehicleRoute " +
                                                                                " set SxWeight = " + newWeight.ToString().Replace(",", ".") + ", SxVolume = " + newVolume.ToString().Replace(",", ".") + ", " +
                                                                                " SxCountRequest = " + newCount + " where SxNSegmentId = '" + data[0] + "' AND SxRequestId='" + requestId + "'", dbConnection);
                                updateRequestVehicle.ExecuteNonQuery();
                            }
                        }
                        return true;
                    }


                    if (segmentUnloadingNumber > 0)
                    {
                        var updateData = new List<List<string>>(); //инициализация
                        var segmentAfterUnloading = "select s.Id, r.SxWeight, r.SxVolume, r.SxCountRequest from SxSegmentInRoute s " +
                                                    " JOIN SxRequestVehicleRoute r on s.Id = r.SxNSegmentId " +
                                                    " where s.SxRouteId = '" + routeId + "' AND r.SxRequestId = '"+requestId+"'" +
                                                    " AND s.SxNumber >" + segmentUnloadingNumber;
                        int i = 0;
                        using (SqlCommand Segment = new SqlCommand(segmentAfterUnloading, dbConnection))
                        {
                            using (SqlDataReader segment = Segment.ExecuteReader())
                            {
                                while (segment.Read())
                                {
                                    updateData.Add(new List<string>());//добавление новой строки
                                    updateData[i].Add(segment.GetValue(0).ToString());
                                    updateData[i].Add(segment.GetValue(1).ToString());
                                    updateData[i].Add(segment.GetValue(2).ToString());
                                    updateData[i].Add(segment.GetValue(3).ToString());
                                    i++;
                                }
                            }
                        }

                        foreach (var data in updateData)
                        {
                            float newWeight = float.Parse(data[1]) - Weight;
                            float newVolume = float.Parse(data[2]) - Volume;
                            int newCount = int.Parse(data[3]) - Count;
                            SqlCommand updateRequestVehicle = new SqlCommand("update SxRequestVehicleRoute " +
                                                                            " set SxWeight = " + newWeight.ToString().Replace(",", ".") + ", SxVolume = " + newVolume.ToString().Replace(",", ".") + ", " +
                                                                            " SxCountRequest = " + newCount + " where SxNSegmentId = '" + data[0] + "' AND SxRequestId='"+requestId+"'", dbConnection);
                            updateRequestVehicle.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Cleverence.Log.Write("Ошибка при пересчете груза в отрезках после выгрузки", ex, Cleverence.Log.LogType.Error);
            }
            return true;
        }

        private int UnknownRequestN = 0;
        private SqlConnection dbConnection;
        public object InvokeMethod(string methodName, object[] args)
        {
            string connStr = @"Data Source=WIN-B9GBQCMSFD9\MSSQL2012;
                            Initial Catalog=M_TransLine;
                            Integrated Security=False;
                            User ID=sa;
                            Password=123456789Aa";
            /*string connStr = @"Data Source=PASHCHENKO;
                            Initial Catalog=M_TransLine;
                            Integrated Security=False;
                            User ID=Supervisor;
                            Password=msden310";*/
            dbConnection = new SqlConnection(connStr);
            /*try
            {
                //пробуем подключится
                dbConnection.Open();
            }
            catch (SqlException ex)
            {
                Cleverence.Log.Write("Ошибка подключения к БД ", ex, Cleverence.Log.LogType.Error);
            }*/

            if (methodName == "GetRoutes")
            {
                //  Получаем данные от клиента
                string userId = args[0] as string;
                string documentTypesName = args[1] as string;
                var WarehouseId = Cleverence.Warehouse.ServerSession.Get().DeviceInfo.WarehouseId;

                /// Метод возвращает не целиком сами документы, а только их заголовки для просмотра на ТСД
                /// (как бы только шапки документов, без строк).
                /// Тело документа будет запрошено по событию ПолучитьДокумент.
                var docs = new Cleverence.Warehouse.DocumentDescriptionCollection();
                if (documentTypesName == "Delivery")
                {
                    // Если операция выдача создаём пустой документ
                    var doc = new Cleverence.Warehouse.DocumentDescription();
                    doc.Id = "Delivery";
                    doc.DocumentTypeName = documentTypesName;
                    doc.Name = "Выдача груза";
                    doc.WarehouseId = WarehouseId;
                    doc.CreateDate = DateTime.Today;
                    doc.Barcode = "1";
                    doc.UserId = userId;
                    docs.Add(doc);
                    return docs;
                }
                else if (documentTypesName == "Moving")
                {
                    // Если операция перемещения создаём пустой документ
                    var doc = new Cleverence.Warehouse.DocumentDescription();
                    doc.Id = "Moving";
                    doc.DocumentTypeName = documentTypesName;
                    doc.Name = "Перемещение";
                    doc.WarehouseId = WarehouseId;
                    doc.CreateDate = DateTime.Today;
                    doc.Barcode = "1";
                    doc.UserId = userId;
                    docs.Add(doc);
                    return docs;
                }else if(documentTypesName == "AllRequest")
                {
                    var doc = new Cleverence.Warehouse.DocumentDescription();
                    doc.Id = Guid.Empty.ToString();
                    doc.DocumentTypeName = documentTypesName;
                    doc.Name = "Текущий склад";
                    doc.WarehouseId = WarehouseId;
                    doc.CreateDate = DateTime.Today;
                    doc.Barcode = "1";
                    doc.UserId = userId;
                    docs.Add(doc);
                    return docs;
                }

                //  Получаем маршруты у которых тип: "Скла-Склад". В отрезках есть текущий склад. Состояние не "Завершен", "В планах", "Отменен"                
                var getRoutesSqlString = "select r.SxNumber, v.SxNumber, r.CreatedOn, r.SxNotes, r.Id " +
                                        " from SxRoute r LEFT JOIN SxVehicle v " +
                                        " ON v.Id = r.SxVehicleId " +
                                        " where r.SxDeliveryTypeId=(select TOP(1) Id from SxDeliveryType where Name='Склад – Склад' OR Id='2B85C7CC-4545-4026-AA5D-FC3DF032EBD4') " +
                                        " AND r.SxStateId = ANY (select Id from SxRouteState where Name != 'В планах' AND Name != 'Завершен' AND Name != 'Отменен') " +
                                        " AND r.Id = ANY (select SxRouteId from SxWarehouseListInRoute where SxWarehouseId = '" + WarehouseId + "') ";

                if (documentTypesName == "LoadingToDoor")
                {
                    getRoutesSqlString = "select r.SxNumber, v.SxNumber, r.CreatedOn, r.SxNotes, r.Id " +
                                        " from SxRoute r LEFT JOIN SxVehicle v " +
                                        " ON v.Id = r.SxVehicleId " +
                                        " where r.SxDeliveryTypeId=(select TOP(1) Id from SxDeliveryType where Name='Склад – Дверь' OR Id='607E6B00-3B1E-4C9E-B30D-B6C2FAED3C29') " +
                                        " AND r.SxStateId = ANY (select Id from SxRouteState where Name != 'В планах' AND Name != 'Завершен' AND Name != 'Отменен') " +
                                        " AND r.Id = ANY (select SxRouteId from SxWarehouseListInRoute where SxWarehouseId = '" + WarehouseId + "') ";

                }
                dbConnection.Open();
                using (SqlCommand Routes = new SqlCommand(getRoutesSqlString, dbConnection))
                {
                    /*Метод ExecuteReader() класса SqlCommand возврашает
                     объект типа SqlDataReader, с помошью которого мы можем
                     прочитать все строки, возврашенные в результате выполнения запроса
                     CommandBehavior.CloseConnection - закрываем соединение после запроса
                     */
                    using (SqlDataReader route = Routes.ExecuteReader())
                    {
                        while (route.Read())
                        {
                            var routeNumber = route.GetValue(0).ToString();
                            var vehicleNumber = route.GetValue(1).ToString();
                            var routeCreateDate = route.GetValue(2);
                            var routeDesc = route.GetValue(3).ToString();
                            var routeId = route.GetValue(4).ToString();

                            var doc = new Cleverence.Warehouse.DocumentDescription();

                            doc.Id = new Guid(routeId).ToString();
                            doc.DocumentTypeName = documentTypesName;
                            doc.Name = routeNumber + " | " + vehicleNumber;
                            doc.WarehouseId = WarehouseId;
                            doc.CreateDate = Convert.ToDateTime(routeCreateDate);
                            doc.Description = routeDesc;
                            doc.UserId = userId;

                            docs.Add(doc);
                        }
                    }
                }
                dbConnection.Close();
                return docs;
            }
            else if (methodName == "GetRequest")
            {
                UnknownRequestN = 0;
                /// Обработка события ПолучитьДокумент.
                /// Чтобы событие сюда пришло, нужно в панели управления проставить в данное событие вызов этого метода
                /// через строку <id коннектора>:GetDocument (например, TestConnector1:GetDocument).

                string userId = args[0] as string;
                string documentId = args[1] as string;
                string documentTypeName = args[2] as string;
                var WarehouseId = Cleverence.Warehouse.ServerSession.Get().DeviceInfo.WarehouseId;

                /// Создаем просто пустой документ.
                var doc = new Cleverence.Warehouse.Document();
                var documentItem = new Cleverence.Warehouse.DocumentItem();

                if (documentTypeName == "Delivery")
                {
                    doc.Id = "Delivery";
                    doc.DocumentTypeName = documentTypeName;
                    doc.Name = "Выдача груза";
                    doc.UserId = userId;
                    doc.WarehouseId = WarehouseId;
                    doc.CreateDate = DateTime.Today;
                    doc.Barcode = documentId;
                    return doc;

                }
                else if (documentTypeName == "Moving")
                {
                    dbConnection.Open();
                    using (SqlCommand Requests = new SqlCommand("select Id, SxNumber, CreatedOn from SxRequest where SxBarCode='" + documentId + "'", dbConnection))
                    {
                        using (SqlDataReader request = Requests.ExecuteReader())
                        {
                            while (request.Read())
                            {
                                doc.Id = request.GetValue(0).ToString();
                                doc.DocumentTypeName = documentTypeName;
                                doc.Name = "Перемещение заявки №" + request.GetValue(1).ToString();
                                doc.UserId = userId;
                                doc.WarehouseId = WarehouseId;
                                doc.CreateDate = Convert.ToDateTime(request.GetValue(2));
                                doc.Description = request.GetValue(1).ToString();
                                doc.Barcode = documentId;
                            }
                        }
                    }
                    var Quantity = "0";
                    using (SqlCommand Requests = new SqlCommand("select COUNT(Id) from SxPackingCargo where SxRequestId='" + doc.Id + "' AND SxCurrentWarehouseId='" + WarehouseId + "'", dbConnection))
                    {
                        using (SqlDataReader request = Requests.ExecuteReader())
                        {
                            while (request.Read())
                            {
                                Quantity = request.GetValue(0).ToString();
                            }
                        }
                    }

                    using (SqlCommand Requests = new SqlCommand("select Id, SxBarcode, SxNumber from SxShelvesOnWarehouses where SxWarehouseId='" + WarehouseId + "'", dbConnection))
                    {
                        using (SqlDataReader request = Requests.ExecuteReader())
                        {
                            while (request.Read())
                            {
                                documentItem.ProductId = request.GetValue(0).ToString();
                                documentItem.DeclaredQuantity = Convert.ToDecimal(Quantity);
                                doc.Description = Quantity;

                                var product = new Cleverence.Warehouse.Product();
                                product.Id = request.GetValue(0).ToString();
                                product.Barcode = request.GetValue(1).ToString();
                                product.Name = request.GetValue(2).ToString();
                                product.Marking = request.GetValue(2).ToString();

                                var packing = new Cleverence.Warehouse.Packing();
                                packing.Id = "Shelf";
                                packing.Name = "Заявка";
                                packing.UnitsQuantity = 1;

                                product.Packings.Add(packing);
                                product.BasePackingId = packing.Id;

                                var productsBook = Cleverence.Warehouse.ProductsBook.Products;
                                if (productsBook.FindById(product.Id) == null)
                                {
                                    Cleverence.Warehouse.ProductsBook.Products.Add(product);
                                }

                                documentItem.Product = product;
                                doc.DeclaredItems.Add(documentItem);
                            }
                        }
                    }
                    dbConnection.Close();
                    return doc;
                }

                doc.Id = documentId;
                doc.DocumentTypeName = documentTypeName;
                doc.UserId = userId;
                doc.WarehouseId = WarehouseId;

                var getRouteSqlString = "select r.SxNumber, r.CreatedOn, v.SxNumber " +
                                        "from SxRoute r LEFT JOIN SxVehicle v ON r.SxVehicleId = v.Id " +
                                        "where r.Id='" + documentId + "' ";
                dbConnection.Open();
                using (SqlCommand Routes = new SqlCommand(getRouteSqlString, dbConnection))
                {
                    using (SqlDataReader route = Routes.ExecuteReader())
                    {
                        while (route.Read())
                        {
                            var routeNumber = route.GetValue(0).ToString();
                            var routeCreateDate = route.GetValue(1);
                            var vehicleNumber = route.GetValue(2).ToString();
                            
                            doc.Name = routeNumber + " | " + vehicleNumber;
                            doc.CreateDate = Convert.ToDateTime(routeCreateDate);
                        }
                    }
                }


                var getRequestSqlString = "";
                if (documentTypeName == "Loading" || documentTypeName == "LoadingToDoor")
                {
                    getRequestSqlString = "select r.Id, r.SxNumber, r.SxNMarking, r.SxBarCode, SUM(p.SxWeight), SUM(p.SxVolume), COUNT(p.Id), r.SxNumberOfSeats from SxRequest r LEFT JOIN SxPackingCargo p ON r.Id= p.SxRequestId " +
                                                "where p.SxCurrentWarehouseId = '" + WarehouseId + "' " +
                                                "AND r.SxRequestStateId != '1EBB0F7B-FC4A-4D9F-B9F2-493E164F2EDD' " +
                                                "AND r.SxRequestStateId != 'ABF7EED9-6152-48AD-8896-1B849D92B21C' " +
                                                "AND r.SxRequestStateId != '86111858-C23B-4A5B-B15D-BB5D0AB67B6E' " +
                                                "GROUP BY r.Id, r.SxNumber, r.SxNMarking, r.SxBarCode, r.SxNumberOfSeats";
                } else if (documentTypeName == "Unloading") {
                    getRequestSqlString = "select r.Id, r.SxNumber, r.SxNMarking, r.SxBarCode, SUM(s.SxWeight), SUM(s.SxVolume), COUNT(s.Id), r.SxNumberOfSeats from SxRequest r " +
                                                " LEFT JOIN SxShippingSheetInRoute s ON s.SxNumberRequestId = r.Id " +
                                                " WHERE s.SxRouteId='" + doc.Id + "' AND s.SxUnloaded!=1 AND s.SxReserved!=1 " +
                                                " AND s.SxUnloadingId=(select Id from SxWarehouseListInRoute where SxWarehouseId='"+ WarehouseId+"' AND SxRouteId='"+ doc.Id+"') " +
                                                " AND r.Id=ANY(select SxNumberRequestId from SxShippingSheetInRoute where SxRouteId='" + doc.Id + "') " +
                                                " GROUP BY r.Id, r.SxNumber, r.SxNMarking, r.SxBarCode, r.SxNumberOfSeats";
                } else if(documentTypeName == "AllRequest")
                {
                    getRequestSqlString = "select r.Id, r.SxNumber, r.SxNMarking, r.SxBarCode, SUM(p.SxWeight), SUM(p.SxVolume), COUNT(p.Id), r.SxNumberOfSeats from SxRequest r LEFT JOIN SxPackingCargo p ON r.Id= p.SxRequestId " +
                                                "where p.SxCurrentWarehouseId = '" + WarehouseId + "' " +
                                                "AND r.SxRequestStateId != '1EBB0F7B-FC4A-4D9F-B9F2-493E164F2EDD' " +
                                                "AND r.SxRequestStateId != 'ABF7EED9-6152-48AD-8896-1B849D92B21C' " +
                                                "AND r.SxRequestStateId != '86111858-C23B-4A5B-B15D-BB5D0AB67B6E' " +
                                                "GROUP BY r.Id, r.SxNumber, r.SxNMarking, r.SxBarCode, r.SxNumberOfSeats";
                }
                if(getRequestSqlString == "") { return false; }
                float allWeight = 0;
                float allVolume = 0;
                using (SqlCommand Requests = new SqlCommand(getRequestSqlString, dbConnection))
                    {
                        using (SqlDataReader request = Requests.ExecuteReader())
                        {
                        if (!request.HasRows)
                        {
                            documentItem.ProductId = "DefaultProduct";
                            documentItem.PackingId = "packing1";
                            documentItem.DeclaredQuantity = 0;
                            doc.DeclaredItems.Add(documentItem);
                        }
                            while (request.Read())
                            {
                                string requestId = request.GetValue(0).ToString();
                                string requstName = request.GetValue(1).ToString();
                                //string requstName = "неизвестный товар";
                                string Marking = request.GetValue(2).ToString();
                                string BarCode = request.GetValue(3).ToString();
                                float weight = float.Parse(request.GetValue(4).ToString());
                                allWeight += weight;
                                float volume = float.Parse(request.GetValue(5).ToString());
                                allVolume += volume;
                                int warehouseCount = int.Parse(request.GetValue(6).ToString());
                                int requestAllCount = int.Parse(request.GetValue(7).ToString());

                                var product = new Cleverence.Warehouse.Product();
                                product.Id = requestId;
                                product.Barcode = BarCode;
                                product.Name = requstName;
                                product.Marking = Marking;
                                product.Fields.Add("weight", weight);
                                product.Fields.Add("volume", volume);
                                product.Fields.Add("warehouseCount", warehouseCount);
                                product.Fields.Add("allCount", requestAllCount);
                                product.Fields.Add("weightUniq", weight / warehouseCount);
                                product.Fields.Add("volumeUniq", volume / warehouseCount);

                                var packing = new Cleverence.Warehouse.Packing();
                                packing.Id = "requestPack";
                                packing.Name = "Заявка";
                                packing.UnitsQuantity = 1;
                                packing.Barcode = BarCode;

                                product.Packings.Add(packing);
                                product.BasePackingId = packing.Id;

                                var productsBook = Cleverence.Warehouse.ProductsBook.Products;
                                if (productsBook.FindById(product.Id) == null)
                                {
                                    Cleverence.Warehouse.ProductsBook.Products.Add(product);
                                }

                                documentItem = new Cleverence.Warehouse.DocumentItem();
                                documentItem.Product = product;
                                documentItem.ProductId = product.Id;
                                documentItem.Packing = packing;
                                documentItem.PackingId = packing.Id;
                                documentItem.DeclaredQuantity = warehouseCount;
                                doc.DeclaredItems.Add(documentItem);

                                doc.Description = "Вес: " + allWeight + " | Объём: "+ allVolume;
                            }
                        }
                    }
                dbConnection.Close();
                return doc;                   
            }
            else if (methodName == "DocumentComplete")
            {
                Cleverence.Log.Write("Принят документ");

                FormsAuthRequest();
                var documentId = args[0] as string;
                var documentsList = Cleverence.Warehouse.DocumentStorage.Documents;
                var currentDocument = documentsList.FindById(documentId);
                if(currentDocument == null)
                {
                    Cleverence.Log.Write("Документ (id: "+ documentId+") в списке DocumentStorage не найден");
                }
                try
                {
                    var productsBook = Cleverence.Warehouse.ProductsBook.Products;
                    int countRequest = 0;
                    if (dbConnection.State != System.Data.ConnectionState.Open)
                    {
                        dbConnection.Open();
                    }
                    var requestListId = new List<string>();
                    foreach (Cleverence.Warehouse.DocumentItem currentItem in currentDocument.CurrentItems)
                    {
                        countRequest++;
                                                
                        #region Перемещение
                            if (currentDocument.DocumentTypeName == "Moving")
                            {
                                var type = " на полку";
                                SqlCommand updateShelfPackingCargo = new SqlCommand("update SxPackingCargo set SxCurrentShelfId='" + currentItem.ProductId + "' " +
                                     "where Id in (select TOP(" + currentItem.CurrentQuantity + ") Id from SxPackingCargo where SxRequestId='" + documentId + "' AND SxCurrentWarehouseId='" + currentDocument.WarehouseId + "' AND SxCurrentShelfId IS NULL)", dbConnection);

                                if (currentDocument.Description == "remove")
                                {
                                    updateShelfPackingCargo = new SqlCommand("update SxPackingCargo set SxCurrentShelfId=null " +
                                                                             "where Id in (select TOP(" + currentItem.CurrentQuantity + ") Id from SxPackingCargo where SxRequestId='" + documentId + "' AND SxCurrentWarehouseId='" + currentDocument.WarehouseId + "' AND SxCurrentShelfId='" + currentItem.ProductId + "')", dbConnection);
                                    type = " с полки";
                                }
                                int countShelfPackingCargo = 0;
                                try
                                {
                                    countShelfPackingCargo = updateShelfPackingCargo.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Ошибка, при выполнении запроса на изменение записи(ей) ", ex, Cleverence.Log.LogType.Error);
                                }
                                if (countShelfPackingCargo < currentItem.CurrentQuantity)
                                {
                                    var shelfNumber = "";
                                    var warehouseName = "";
                                    //  имя склада и покли
                                    using (SqlCommand Names = new SqlCommand("select w.Name, s.SxNumber from SxWarehouses w, SxShelvesOnWarehouses s where w.Id='" + currentDocument.WarehouseId + "' AND s.Id='" + currentItem.ProductId + "'", dbConnection))
                                    {
                                        using (SqlDataReader name = Names.ExecuteReader())
                                        {
                                            while (name.Read())
                                            {
                                                warehouseName = name.GetValue(0).ToString();
                                                shelfNumber = name.GetValue(1).ToString();
                                            }
                                        }
                                    }
                                    //  Update Request.Note
                                    SqlCommand insertRequestNote = new SqlCommand("update SxRequest set SxNotes= " +
                                                                                "(select SxNotes from SxRequest where Id='" + documentId + "')+ " +
                                                                                "'<div> !!! Было произведено не корректное перемещение на складе " + warehouseName + type + " №" + shelfNumber + " в количестве " + (currentItem.CurrentQuantity - countShelfPackingCargo) + "шт </div>' " +
                                                                                "where Id='" + documentId + "'", dbConnection);
                                    insertRequestNote.ExecuteNonQuery();

                                }
                                productsBook.Remove(productsBook.FindById(currentItem.ProductId));
                                documentsList.Remove(currentDocument);
                                return true;
                            }
                        #endregion
                        #region Выгрузка
                        if (currentDocument.DocumentTypeName == "Unloading")
                        {
                            #region Новая заявка
                            var requestId = "";
                            var code = currentItem.Fields.FindByFieldName("code");
                            if (currentItem.SSCC != null)
                            {
                                Cleverence.Log.Write("Выгрузка новой заявки. \n");
                                var Barcode = currentItem.SSCC;
                                var match = Regex.Match(Barcode, @"[0-9][0-9]+(?:\.[0-9]*)?");
                                if (!match.Success)
                                {
                                    Cleverence.Log.Write("Не верный ШК: " + Barcode + " \n");
                                }
                                else
                                {
                                    Barcode = match.Value;
                                }

                                Cleverence.Log.Write("Ищу заявку с ШК:" + Barcode + " \n");
                                var requestNumber = "";
                                using (SqlCommand Requests = new SqlCommand("select Id, SxNumber from SxRequest where SxBarCode='" + Barcode + "'", dbConnection))
                                {
                                    using (SqlDataReader requestData = Requests.ExecuteReader())
                                    {
                                        while (requestData.Read())
                                        {
                                            requestId = requestData.GetValue(0).ToString();
                                            requestNumber = requestData.GetValue(1).ToString();
                                        }
                                    }
                                }

                                var warehouseName = "";
                                using (SqlCommand Requests = new SqlCommand("select Name from SxWarehouses where Id='" + currentDocument.WarehouseId + "'", dbConnection))
                                {
                                    using (SqlDataReader requestData = Requests.ExecuteReader())
                                    {
                                        while (requestData.Read())
                                        {
                                            warehouseName = requestData.GetValue(0).ToString();
                                        }
                                    }
                                }

                                if (requestId == "")
                                {
                                    Cleverence.Log.Write("Заявка с ШК: " + Barcode + " не найдена. \n");
                                    // Добавляем примечание в текущий маршрут
                                    SqlCommand insertRouteNotes = new SqlCommand("update SxRoute set SxNotes=" +
                                                                                "(select SxNotes from SxRoute where Id='" + documentId + "')+ " +
                                                                                "'<div>Заявка со штрихкодом " + Barcode + " была выгружена в количестве " + currentItem.CurrentQuantity + "шт из текущего маршрута на склад " + warehouseName + "</div>' " +
                                                                                " where Id='" + documentId + "\n'", dbConnection);
                                    insertRouteNotes.ExecuteNonQuery();
                                    continue;
                                }
                                /*else {

                                    Cleverence.Log.Write("Заявка найдена: " + requestNumber + " \n");
                                    //  Инсерт в Заявка.НЕ КОРРЕКТНАЯ РАЗГРУЗКА/ПОГРУЗКА
                                    SqlCommand insertIncorrect = new SqlCommand(" insert into SxIncorrectLoadUnload (SxRequestId, SxRouteId, SxUnloadingExceededBy) " +
                                                                                " VALUES ('" + requestId + "', '" + documentId + "', " + currentItem.CurrentQuantity + ")", dbConnection);
                                    insertIncorrect.ExecuteNonQuery();
                                    // Добавляем примечание в текущий маршрут
                                    SqlCommand insertRouteNotes = new SqlCommand("update SxRoute set SxNotes=" +
                                                                                "(select SxNotes from SxRoute where Id='" + documentId + "')+ " +
                                                                                "'<div>Заявка № <a href=\"/0/Nui/ViewModule.aspx#SectionModuleV2/SxRequestSection/SxRequestPage/edit/" + requestId + "\">" + requestNumber + "</a> была выгружена в количестве " + currentItem.CurrentQuantity + "шт из текущего маршрута на склад " + warehouseName + "</div>' " +
                                                                                " where Id='" + documentId + "\n'", dbConnection);
                                    insertRouteNotes.ExecuteNonQuery();
                                }*/
                            }
                            #endregion

                            //Выгрузка на склад
                            Cleverence.Log.Write("Выгрузка на склад. Кол-во: "+ currentItem.CurrentQuantity);

                            if(currentItem.ProductId == "*" && requestId != "")
                            {
                                currentItem.ProductId = requestId;
                            }
                            
                            var unloading = "";
                            using (SqlCommand Names = new SqlCommand("select Id from SxWarehouseListInRoute where SxWarehouseId='" + currentDocument.WarehouseId + "' AND SxRouteId='" + documentId + "'", dbConnection))
                            {
                                using (SqlDataReader name = Names.ExecuteReader())
                                {
                                    while (name.Read())
                                    {
                                        unloading = name.GetValue(0).ToString();
                                    }
                                }
                            }

                            float weight = 0;
                            float volume = 0;
                            string unloadingOld = "";
                            using (SqlCommand Count = new SqlCommand("select SUM(SxWeight), SUM(SxVolume), SxUnloadingId from " +
                                                                    " (select TOP " + currentItem.CurrentQuantity + " SxWeight, SxVolume, SxUnloadingId from SxShippingSheetInRoute" +
                                                                    " where SxNumberRequestId = '" + currentItem.ProductId + "' " +
                                                                    " AND SxRouteId = '" + documentId + "' " +
                                                                    " AND SxReserved != 1 AND SxUnloaded != 1) as Params " +
                                                                    " GROUP BY SxUnloadingId", dbConnection))
                            {
                                using (SqlDataReader shipping = Count.ExecuteReader())
                                {
                                    while (shipping.Read())
                                    {
                                        weight = float.Parse(shipping.GetValue(0).ToString());
                                        volume = float.Parse(shipping.GetValue(1).ToString());
                                        unloadingOld = shipping.GetValue(2).ToString();
                                    }
                                }
                            }

                            var countShippingSheet = 0;
                            SqlCommand updateShippingSheet = new SqlCommand("update SxShippingSheetInRoute " +
                                                                            " set SxUnloaded=1, " +
                                                                            " SxUnloadingId='" + unloading + "' " +
                                                                            " where Id in (select TOP(" + currentItem.CurrentQuantity + ") Id from SxShippingSheetInRoute " +
                                                                            " where SxNumberRequestId='" + currentItem.ProductId + "' " +
                                                                            " AND SxRouteId='" + documentId + "' " +
                                                                            " AND SxReserved!=1 AND SxUnloaded!=1 ORDER BY CreatedOn ASC)", dbConnection);
                            countShippingSheet += updateShippingSheet.ExecuteNonQuery();

                            //  Пересчет груза в отрезках
                            Cleverence.Log.Write("Пересчет груза в отрезках начал");
                            checkUnloading(unloading, unloadingOld, documentId, currentItem.ProductId, int.Parse(currentItem.CurrentQuantity.ToString()), weight, volume);                            
                            Cleverence.Log.Write("Закончил пересчет груза в отрезках");

                            //  Update Packign Cargo
                            var requestState = "62accb54-5b41-4922-bd69-f173f7854f54";
                            using (SqlCommand Names = new SqlCommand("select SxAffiliateId from SxWarehouses where Id=(select SxDueWarehouseId from SxRequest where Id='" + currentItem.ProductId + "') AND Id='" + currentDocument.WarehouseId + "'", dbConnection))
                            {
                                using (SqlDataReader name = Names.ExecuteReader())
                                {
                                    while (name.Read())
                                    {
                                        requestState = "86111858-c23b-4a5b-b15d-bb5d0ab67b6e";
                                    }
                                }
                            }

                            var requestEntrepot = "true";
                            using (SqlCommand Names = new SqlCommand("select SxAffiliateId from SxWarehouses " +
                                                                    " where (Id=(select SxStartWarehouseId from SxRequest where Id='" + currentItem.ProductId + "') " +
                                                                    " AND Id='" + currentDocument.WarehouseId + "') " +
                                                                    " OR (Id = (select SxDueWarehouseId from SxRequest where Id='" + currentItem.ProductId + "') " +
                                                                    " AND Id = '" + currentDocument.WarehouseId + "')", dbConnection))
                            {
                                using (SqlDataReader name = Names.ExecuteReader())
                                {
                                    while (name.Read())
                                    {
                                        requestEntrepot = "false";
                                    }
                                }
                            }
                            
                            var shelves = "";
                            if (code != null)
                            {
                                using (SqlCommand ShelvesSelect = new SqlCommand("select Id from SxShelvesOnWarehouses " +
                                                                        " where SxWarehouseId='" + currentDocument.WarehouseId + "'" +
                                                                        " AND SxBarcode='" + code.Value + "'", dbConnection))
                                {
                                    using (SqlDataReader shelvesResp = ShelvesSelect.ExecuteReader())
                                    {
                                        while (shelvesResp.Read())
                                        {
                                            shelves = shelvesResp.GetValue(0).ToString();
                                        }
                                    }
                                }
                            }
                                SqlCommand updatePackingCargo = new SqlCommand("update SxPackingCargo " +
                                                                              " set SxCurrentRouteId=null, " +
                                                                              " SxCurrentWarehouseId='" + currentDocument.WarehouseId + "', " +
                                                                              " SxCurrentShelfId='" + shelves + "', " +
                                                                              " SxStateId='" + requestState + "', " +
                                                                              " SxEntrepot =" + (requestEntrepot == "true" ? "1" : "0") +
                                                                              " where Id in (select TOP(" + currentItem.CurrentQuantity + ") Id from SxPackingCargo " +
                                                                              " where SxRequestId='" + currentItem.ProductId + "' " +
                                                                              " AND SxCurrentRouteId='" + documentId + "'" +
                                                                              " AND SxReserved!=1 ORDER BY CreatedOn ASC)", dbConnection);
                                var count = updatePackingCargo.ExecuteNonQuery();
                            Cleverence.Log.Write("Выгрузка на склад. Обновили фасовку. Кол-во: " + count);
                            if(requestListId.IndexOf(currentItem.ProductId) == -1) requestListId.Add(currentItem.ProductId);

                            if (countShippingSheet > 0)
                            {
                                if (countShippingSheet < currentItem.CurrentQuantity)
                                {
                                    //  Инсерт в Заявка.НЕ КОРРЕКТНАЯ РАЗГРУЗКА/ПОГРУЗКА
                                    SqlCommand insertIncorrect = new SqlCommand(" insert into SxIncorrectLoadUnload (SxRequestId, SxRouteId, SxUnloadingExceededBy) " +
                                                                                " VALUES ('" + currentItem.ProductId + "', '" + documentId + "', " + (currentItem.CurrentQuantity - countShippingSheet) + ")", dbConnection);
                                    insertIncorrect.ExecuteNonQuery();
                                }
                            }
                            productsBook.Remove(productsBook.FindById(currentItem.ProductId));
                        }
                        #endregion
                        #region Загрузка  
                        if (currentDocument.DocumentTypeName == "Loading" || currentDocument.DocumentTypeName == "LoadingToDoor")
                        {
                            #region Новая заявка    
                            var Barcode = currentItem.Product.Barcode;
                            var warehouseName = "";
                            if (currentItem.SSCC != null)
                            {
                                Cleverence.Log.Write("Загрузка новой заявки. \n");
                                var match = Regex.Match(currentItem.SSCC, @"[0-9][0-9]+(?:\.[0-9]*)?");
                                if (!match.Success)
                                {
                                    Cleverence.Log.Write("Не верный ШК: " + Barcode + " \n");
                                }
                                else
                                {
                                    Barcode = match.Value;
                                }

                                Cleverence.Log.Write("Ищу заявку с ШК:" + Barcode + " \n");
                                using (SqlCommand Requests = new SqlCommand("select Id, SxNumber from SxRequest where SxBarCode='" + Barcode + "'", dbConnection))
                                {
                                    using (SqlDataReader requestData = Requests.ExecuteReader())
                                    {
                                        while (requestData.Read())
                                        {
                                            currentItem.ProductId = requestData.GetValue(0).ToString();
                                            //currentItem.Product.Name = requestData.GetValue(1).ToString();
                                        }
                                    }
                                }
                                using (SqlCommand Requests = new SqlCommand("select Name from SxWarehouses where Id='" + currentDocument.WarehouseId + "'", dbConnection))
                                {
                                    using (SqlDataReader requestData = Requests.ExecuteReader())
                                    {
                                        while (requestData.Read())
                                        {
                                            warehouseName = requestData.GetValue(0).ToString();
                                        }
                                    }
                                }
                            }
                            #endregion
                            if(currentItem.ProductId == "*")
                            {
                                Cleverence.Log.Write("Заявка не найдена \n");
                                var insertRouteNotes = new SqlCommand("update SxRoute set SxNotes=" +
                                                                  "(select SxNotes from SxRoute where Id='" + documentId + "')+ " +
                                                                  "'<div>Заявка со штрихкодом " + Barcode + " была загружена в количестве " + currentItem.CurrentQuantity + "шт в текущий маршрута со склада " + warehouseName + "</div>' " +
                                                                  " where Id='" + documentId + "\n'", dbConnection);
                                insertRouteNotes.ExecuteNonQuery();
                                continue;
                            }

                            //Cleverence.Log.Write("Загрузка. Номер заявки " + currentItem.Product.Name + " \n");
                            if (currentDocument.DocumentTypeName == "Loading")
                            {
                                Cleverence.Log.Write("Выполняю загрузку груза в количестве " + currentItem.CurrentQuantity + " шт");
                                currentDocument.Description = currentItem.ProductId;
                                try
                                {
                                    bool lastRequest = false;
                                    if (countRequest == currentDocument.CurrentItems.Count)
                                    {
                                        lastRequest = true;
                                    }
                                    Loading(currentDocument, (int)currentItem.CurrentQuantity, lastRequest);
                                }
                                catch (Exception ex)
                                {
                                    Cleverence.Log.Write("Ошибка при загрузке ", ex, Cleverence.Log.LogType.Error);
                                    dbConnection.Close();
                                }
                            }
                            else if (currentDocument.DocumentTypeName == "LoadingToDoor")
                            {
                                Cleverence.Log.Write("Выполняю загрузку груза до двери \n");
                                currentDocument.Description = currentItem.ProductId;
                                try
                                {
                                    dbConnection.Open();
                                    LoadingToDoor(currentDocument, (int)currentItem.CurrentQuantity, currentItem.Product.Name);
                                    dbConnection.Close();
                                }
                                catch (Exception ex)
                                {
                                    Cleverence.Log.Write("Ошибка при загрузке до двери ", ex, Cleverence.Log.LogType.Error);
                                    dbConnection.Close();
                                }
                            }                       
                        }
                        #endregion
                        Cleverence.Log.Write("Удаляю заявку из productsBook \n");
                        productsBook.Remove(currentItem.Product);
                    }
                    recalcPackingCargo(requestListId);

                    Cleverence.Log.Write("Закрываю соединение с БД \n");
                    dbConnection.Close();
                    Cleverence.Log.Write("Удаляю документ из documentsList \n");
                    documentsList.Remove(currentDocument);
                    return true;
                }
                catch (Exception ex)
                {
                    Cleverence.Log.Write("Ошибка при разборе документа ", ex, Cleverence.Log.LogType.Error);
                    documentsList.Remove(currentDocument);
                    return false;
                }
            }
            else if (methodName == "GetRequestParams")
            {
                try
                {
                    Cleverence.Warehouse.InvokeArgs arguments = args[0] as Cleverence.Warehouse.InvokeArgs;
                    var Barcode = arguments.GetArg("Barcode").ToString();

                    var match = Regex.Match(Barcode, @"[0-9][0-9]+(?:\.[0-9]*)?");
                    if (!match.Success)
                    {
                        Cleverence.Log.Write("Не верный ШК: " + Barcode + " \n");
                    }
                    else
                    {
                        Barcode = match.Value;
                    }

                    Cleverence.Log.Write("Загрузка. Ищу заявку с ШК:" + Barcode + " \n");
                    var Quantity = Convert.ToDecimal(arguments.GetArg("Quantity"));
                    var DeviceId = arguments.GetArg("DeviceId").ToString();
                    var WarehouseId = arguments.GetArg("WarehouseId").ToString();

                    var marking = "";
                    var requestId = "";
                    dbConnection.Open();
                    using (SqlCommand Requests = new SqlCommand("select SxNMarking, Id from SxRequest where SxBarCode='" + Barcode + "'", dbConnection))
                    {
                        using (SqlDataReader request = Requests.ExecuteReader())
                        {
                            while (request.Read())
                            {
                                marking = request.GetValue(0).ToString();
                                requestId = request.GetValue(1).ToString();
                            }
                        }
                    }

                    if (requestId == "") return false;

                    int count = 0;
                    using (SqlCommand Packing = new SqlCommand("select count(Id) from SxPackingCargo where SxRequestId='" + requestId + "' AND SxCurrentWarehouseId='"+WarehouseId+"'", dbConnection))
                    {
                        using (SqlDataReader packing = Packing.ExecuteReader())
                        {
                            while (packing.Read())
                            {
                                count = Convert.ToInt16(packing.GetValue(0));
                            }
                        }
                    }
                    dbConnection.Close();

                    var requestDoc = new Cleverence.Warehouse.Document();

                    var requestInfo = new Cleverence.Warehouse.Product();
                    requestInfo.Id = requestId;
                    requestInfo.Barcode = Barcode;
                    requestInfo.Name = marking;
                    requestInfo.Marking = "заявка";

                    //requestDoc.DeclaredItems.Product = requestInfo;
                    //requestDoc.DeclaredQuantity = count;
                    return requestDoc;

                    /*var packing = new Cleverence.Warehouse.Packing();
                    packing.Id = "requestPack";
                    packing.Name = "Заявка";
                    packing.UnitsQuantity = Quantity;

                    product.Packings.Add(packing);
                    product.BasePackingId = packing.Id;

                    var productsBook = Cleverence.Warehouse.ProductsBook.Products;
                    if (productsBook.FindById(product.Id) != null)
                    {
                        productsBook.Remove(productsBook.FindById(product.Id));
                    }
                    Cleverence.Warehouse.ProductsBook.Products.Add(product);
                    Cleverence.Log.Write("Создана новая заявка UnknownRequest" + UnknownRequestN + " с штрихкодом " + Barcode + " \n");
                    return true;*/
                }
                catch (Exception ex)
                {
                    Cleverence.Log.Write("Ошибка создания новой заявки ", ex, Cleverence.Log.LogType.Error);
                    return false;
                }
            }
            else if (methodName == "GeRequestInfo")
            {
                Cleverence.Warehouse.InvokeArgs param = args[0] as Cleverence.Warehouse.InvokeArgs;
                var barcode = param.GetArg("barcode").ToString();
                var match = Regex.Match(barcode, @"[0-9][0-9]+(?:\.[0-9]*)?");
                if (!match.Success)
                {
                    Cleverence.Log.Write("Не верный штрихкод " + barcode + " \n");
                }
                else
                {
                    barcode = match.Value;
                }
                string message = "";
                string requestInfoSqlString = "DECLARE @ShipperAccount nvarchar(30) " +
                                            " SELECT @ShipperAccount = (SELECT Name FROM Account  " +
                                            " WHERE id=(select SxShipperAccountId from SxRequest where SxBarCode='" + barcode + "')) " +
                                            " DECLARE @ConsigneeAccount nvarchar(30)  " +
                                            " SELECT @ConsigneeAccount = (SELECT Name FROM Account  " +
                                            " WHERE id=(select SxConsigneeAccountId from SxRequest where SxBarCode='" + barcode + "'))  " +
                                            " DECLARE @ShipperContact nvarchar(30)  " +
                                            " SELECT @ShipperContact = (SELECT Name FROM Contact  " +
                                            " WHERE id=(select SxShipperContactId from SxRequest where SxBarCode='" + barcode + "')) " +
                                            " DECLARE @ConsigneeContact nvarchar(30)  " +
                                            " SELECT @ConsigneeContact = (SELECT Name FROM Contact  " +
                                            " WHERE id=(select SxConsigneeContactId from SxRequest where SxBarCode='" + barcode + "'))  " +
                                            " select SxCanBeIssued, @ShipperAccount, @ShipperContact, @ConsigneeAccount,  @ConsigneeContact " +
                                            " from SxRequest where SxBarCode='" + barcode + "'";
                dbConnection.Open();
                using (SqlCommand Requests = new SqlCommand(requestInfoSqlString, dbConnection))
                {
                    using (SqlDataReader request = Requests.ExecuteReader())
                    {
                        while (request.Read())
                        {
                            var issued = (request.GetValue(0).ToString() == "True") ? "да" : "нет";
                            var shipper = request.GetValue(1).ToString();
                            shipper += " " + request.GetValue(2).ToString();
                            var consignee = request.GetValue(3).ToString();
                            consignee += " " + request.GetValue(4).ToString();

                            message += "<b>Выдавать:</b> " + issued + "<br/><b>Грузоотправитель:</b> " + shipper + "<br/><b>Грузополучатель:</b> " + consignee;                            
                            return message;
                        }
                    }
                }
                dbConnection.Close();
                return null;
            }
            else if (methodName == "RequestComplite")
            {
                Cleverence.Warehouse.InvokeArgs param = args[0] as Cleverence.Warehouse.InvokeArgs;
                var barcode = param.GetArg("barcode").ToString();
                var match = Regex.Match(barcode, @"[0-9][0-9]+(?:\.[0-9]*)?");
                if (!match.Success)
                {
                    Cleverence.Log.Write("Не верный штрихкод " + barcode + " \n");
                }
                else
                {
                    barcode = match.Value;
                }
                dbConnection.Open();
                SqlCommand updateRequestState = new SqlCommand("update SxRequest " +
                                                               "set SxRequestStateId=(select Id from SxRequestState where id='1EBB0F7B-FC4A-4D9F-B9F2-493E164F2EDD' OR Name='Завершена') " +
                                                               "where SxBarCode='" + barcode + "'", dbConnection);

                updateRequestState.ExecuteNonQuery();
                dbConnection.Close();

            }
            else if (methodName == "countCargoOnShelf")
            {
                Cleverence.Warehouse.InvokeArgs param = args[0] as Cleverence.Warehouse.InvokeArgs;
                var requestBarcode = param.GetArg("barcode").ToString();
                var match = Regex.Match(requestBarcode, @"[0-9][0-9]+(?:\.[0-9]*)?");
                if (!match.Success)
                {
                    Cleverence.Log.Write("Не верный штрихкод " + requestBarcode + " \n");
                }
                else
                {
                    requestBarcode = match.Value;
                }

                var shelf = param.GetArg("shelf");
                var type = param.GetArg("type");

                var selectString = "select COUNT(Id) from SxPackingCargo where SxRequestId=(select Id from SxRequest where SxBarCode='" + requestBarcode + "') AND SxCurrentShelfId = '" + shelf + "' ";
                if (type.ToString() == "put")
                {
                    selectString = "select COUNT(Id) from SxPackingCargo where SxRequestId=(select Id from SxRequest where SxBarCode='" + requestBarcode + "') AND SxCurrentShelfId IS NULL ";
                }

                var count = 0;
                dbConnection.Open();
                using (SqlCommand Requests = new SqlCommand(selectString, dbConnection))
                {
                    using (SqlDataReader request = Requests.ExecuteReader())
                    {
                        while (request.Read())
                        {
                            count = Convert.ToInt32(request.GetValue(0).ToString());
                        }
                    }
                }
                dbConnection.Close();
                return count;
            }
            else if (methodName == "CheckShelves")
            {
                Cleverence.Log.Write("Проверка наличия полки");
                var WarehouseId = Cleverence.Warehouse.ServerSession.Get().DeviceInfo.WarehouseId;
                Cleverence.Warehouse.InvokeArgs arguments = args[0] as Cleverence.Warehouse.InvokeArgs;
                var shelvesBarcode = arguments.GetArg("shelves").ToString();
                Cleverence.Log.Write("Штрих код полки: "+ shelvesBarcode + " \n");
                var result = false;
                dbConnection.Open();
                using (SqlCommand Shelves = new SqlCommand("select SxNumber from SxShelvesOnWarehouses where SxWarehouseId='" + WarehouseId + "' AND SxBarcode='"+ shelvesBarcode + "'", dbConnection))
                {
                    using (SqlDataReader shelves = Shelves.ExecuteReader())
                    {
                        while (shelves.Read())
                        {
                            result = true;
                        }
                    }
                }
                dbConnection.Close();
                Cleverence.Log.Write("Результат: " + result + " \n");
                return result;
            }
            else if (methodName == "checkWarehouse")
            {
                Cleverence.Log.Write("Проверка склада в маршруте");
                Cleverence.Warehouse.InvokeArgs arguments = args[0] as Cleverence.Warehouse.InvokeArgs;
                var requestId = arguments.GetArg("requestId").ToString();
                var documentId = Cleverence.Warehouse.ServerSession.Get().DeviceInfo.DocumentId;
                string result = "";
                string dueWarehouse = "";
                dbConnection.Open();
                using (SqlCommand isWarehouse = new SqlCommand("select w.Name, wR.Id from SxWarehouses w "+
                                                               "JOIN SxRequest r ON w.Id = r.SxDueWarehouseId "+
                                                               "LEFT OUTER JOIN SxWarehouseListInRoute wR ON r.SxDueWarehouseId = wR.SxWarehouseId AND wR.SxRouteId = '"+ documentId + "' "+
                                                               "where r.Id = '"+ requestId + "'", dbConnection))
                {
                    using (SqlDataReader warehouse = isWarehouse.ExecuteReader())
                    {
                        while (warehouse.Read())
                        {
                            result = warehouse.GetValue(1).ToString();
                            dueWarehouse = warehouse.GetValue(0).ToString();
                        }
                    }
                }
                dbConnection.Close();
                Cleverence.Log.Write("Результат: " + result + " \n");

                if (result == "")
                {
                    return dueWarehouse;
                }  else {
                    return null;
                }

            }
            return null;
        }

        /// <summary>
        /// Ограничение на время выполнения вызова.  Если время обработки InvokeMethod вревысит Timeout в секундах, то 
        /// сервер Mobile SMARTS поступит в соответствии с TimeoutBehavaior.
        /// </summary>
        public int Timeout
        {
            get;
            set;
        }

        /// <summary>
        /// При превышении времени ожидания выполнения вызова сервер Mobile SMARTS сгенерирует исключение.
        /// Вторая альтернатива - прервать и попробовать вызов еще раз (Cleverence.Connectivity.TimeoutBehavaior.ReInvoke).
        /// </summary>
        private Cleverence.Connectivity.TimeoutBehavaior timeoutBehavaior = Cleverence.Connectivity.TimeoutBehavaior.ThrowException;
        public Cleverence.Connectivity.TimeoutBehavaior TimeoutBehavaior
        {
            get { return this.timeoutBehavaior; }
            set
            {
                this.timeoutBehavaior = value;
            }
        }

        #endregion
    }
}