/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.dynamicCatalog;

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.spi.ConnectorId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.codehaus.plexus.util.FileUtils;
import org.postgresql.Driver;
import org.postgresql.PGNotification;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.google.common.base.Strings.nullToEmpty;

/*
import org.codehaus.plexus.util.FileUtils;
import org.postgresql.Driver;
import org.postgresql.PGNotification;
 */

@Path("/v1/catalog")
public class DynamicCatalogController
{
    private final CatalogManager catalogManager;
    private final ConnectorManager connectorManager;
    private final Announcer announcer;
    private final InternalNodeManager internalNodeManager;
    private final File catalogConfigurationDir;
    private static final Logger log = Logger.get(DynamicCatalogController.class);
    private final ResponseParser responseParser;
    private Listener listener;

    private DynamicCatalogController(CatalogManager catalogManager, ConnectorManager connectorManager, Announcer announcer, InternalNodeManager internalNodeManager, File catalogConfigurationDir, ResponseParser responseParser)
    {
        this.catalogManager = catalogManager;
        this.connectorManager = connectorManager;
        this.announcer = announcer;
        this.internalNodeManager = internalNodeManager;
        this.responseParser = responseParser;
        // clean catalogConfigurationDir upon start
        // this is called before catalogs can even be loaded into memory
        // (so this is sufficient to handle pre-existing catalogs on startup)
        this.catalogConfigurationDir = catalogConfigurationDir;
        try {
            FileUtils.cleanDirectory(this.catalogConfigurationDir);
            System.out.println("cleaned catalog dir");
        }
        catch (IOException ioe) {
            System.out.println("ERROR Unable to clean catalog dir");
        }

        // init listener, listener's constructor will query db and update catalog dir
        try {
            this.listener = new Listener("jdbc:postgresql://db.sece.io/geonaming?" +
                    "user=geo_api" +
                    "&password=g3QMmAsv" +
                    "&sslmode=require", this);
            this.listener.start();
        }
        catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    @Inject
    public DynamicCatalogController(CatalogManager catalogManager, ConnectorManager connectorManager, Announcer announcer, InternalNodeManager internalNodeManager, StaticCatalogStoreConfig config, ResponseParser responseParser)
    {
        this(catalogManager, connectorManager, announcer, internalNodeManager, config.getCatalogConfigurationDir(), responseParser);
    }

    @Path("add")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addCatalog(CatalogVo catalogVo)
    {
        try {
            log.info("addCatalog : input values are " + catalogVo);
            ConnectorId connectorId = connectorManager.createConnection(catalogVo.getCatalogName(), catalogVo.getConnectorName(), catalogVo.getProperties());
            updateConnectorIdAnnouncement(announcer, connectorId, internalNodeManager);
            log.info("addCatalog() : catalogConfigurationDir: " + catalogConfigurationDir.getAbsolutePath());
            writeToFile(catalogVo);
            log.info("addCatalog() : Successfully added catalog " + catalogVo.getCatalogName());
            return successResponse(responseParser.build("Successfully added catalog: " + catalogVo.getCatalogName(), 200));
        }
        catch (Exception ex) {
            log.error("addCatalog() : Error adding catalog " + ex.getMessage());
            return failedResponse(responseParser.build("Error adding Catalog: " + ex.getMessage(), 500));
        }
    }

    @Path("delete")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteCatalog(@QueryParam("catalogName") String catalogName)
    {
        log.info("deleteCatalog(): Deleting catalog: " + catalogName);
        String responseMessage = "";
        if (catalogManager.getCatalog(catalogName).isPresent()) {
            log.info("deleteCatalog() : Catalog exists so deleting catalog " + catalogName);
            connectorManager.dropConnection(catalogName);
            responseMessage = deletePropertyFile(catalogName) ? "Successfully deleted" : "Error deleting catalog";
        }
        else {
            log.info("deleteCatalog() : Catalog doesn't exist, Can't be deleted " + catalogName);
            return failedResponse(responseParser.build("Catalog doesn't exists: " + catalogName, 500));
        }
        log.info("deleteCatalog() : successfully deleted catalog " + catalogName);
        return successResponse(responseParser.build(responseMessage + " : " + catalogName, 200));
    }

    private boolean deletePropertyFile(String catalogName)
    {
        return new File(getPropertyFilePath(catalogName)).delete();
    }

    private void writeToFile(CatalogVo catalogVo)
            throws Exception
    {
        final Properties properties = new Properties();
        properties.put("connector.name", catalogVo.getConnectorName());
        properties.putAll(catalogVo.getProperties());
        String filePath = getPropertyFilePath(catalogVo.getCatalogName());
        log.info("filepath: " + filePath);
        File propertiesFile = new File(filePath);
        try (OutputStream out = new FileOutputStream(propertiesFile)) {
            properties.store(out, "adding catalog using endpoint");
        }
        catch (Exception ex) {
            log.error("error while writing to a file :" + ex.getMessage());
            throw new Exception("Error writing to file " + ex.getMessage());
        }
    }

    private String getPropertyFilePath(String catalogName)
    {
        return catalogConfigurationDir.getPath() + File.separator + catalogName + ".properties";
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, ConnectorId connectorId, InternalNodeManager nodeManager)
    {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
        nodeManager.refreshNodes();
    }

    private Response successResponse(ResponseParser responseParser)
    {
        return Response.status(Response.Status.OK).entity(responseParser).type(MediaType.APPLICATION_JSON).build();
    }

    private Response failedResponse(ResponseParser responseParser)
    {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseParser).type(MediaType.APPLICATION_JSON).build();
    }

    class Listener
            extends Thread
    {
        private final java.sql.Connection conn;
        private final org.postgresql.PGConnection pgconn;
        private final Driver driver = new Driver();
        private final String channel = "nodes";
        private final ObjectMapper mapper;
        private final DynamicCatalogController controller;
        private LongSummaryStatistics stats;
        private boolean synced;

        Listener(String connectionString, DynamicCatalogController controller) throws SQLException
        {
            this.controller = controller;
            this.conn = this.driver.connect(connectionString, new Properties());
            this.pgconn = this.conn.unwrap(org.postgresql.PGConnection.class);
            this.mapper = new ObjectMapper();
            this.stats = new LongSummaryStatistics();
            this.synced = false;
            java.sql.Statement stmt = this.conn.createStatement();
            int setuid = stmt.executeUpdate("set session.uid='R2wzx2ovqEeBSjcRxgPZ9ZNT2j33';");
            System.out.println(setuid);
            stmt.execute("LISTEN " + this.channel);
            stmt.close();
            System.out.println("Listener initiated on channel: " + this.channel);
        }

        public void run()
        {
            try {
                while (true) {
                    System.out.println("Listener loop");
                    if (!this.synced) {
                        java.sql.Statement stmt = this.conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT id, url from nodes");
                        int catalogCount = 0;

                        long start = System.currentTimeMillis();
                        while (rs.next()) {
                            if (insertCatalog(rs.getString(1), rs.getString(2))) {
                                catalogCount++;
                            }
                        }
                        long end = System.currentTimeMillis();
                        this.stats.accept(end - start);

                        if (catalogCount > 0) {
                            System.out.println("added " + catalogCount + " catalogs on sync with localdb");
                            this.synced = true;
                        }
                        else {
                            System.out.println("no updates on sync with localdb");
                        }
                        System.out.println("time to sync: " + (end - start));

                        stmt.close();
                        rs.close();
                    }
                    org.postgresql.PGNotification[] notifications = pgconn.getNotifications(45000);
                    // If this thread is the only one that uses the connection, a timeout can be used to
                    // receive notifications immediately:
                    // org.postgresql.PGNotification notifications[] = pgconn.getNotifications(10000);

                    if (notifications != null) {
                        for (int i = 0; i < notifications.length; i++) {
                            this.processNotification(notifications[i]);
                            //{"action" : "insert", "id" : "aaa", "url" : "blah",
                            // "service_region" : {"type": "Polygon", "coordinates": [[[-71.177658505, 42.390290974], [-71.177682027, 42.390370174], [-71.177606301, 42.390382566], [-71.177582658, 42.390303365], [-71.177658505, 42.390290974]]]}}
                        }
                        this.synced = true;
                    }
                    // wait a while before checking again for new
                    // notifications

//                Thread.sleep(500);
                }
            }
            catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }

        private void processNotification(PGNotification n)
        {
            System.out.println("Got notification: " + n.getName());
            System.out.println(n.getParameter());
            try {
                Map<String, String> payload = mapper.readValue(n.getParameter(), Map.class);
                switch (payload.get("action")) {
                    case "insert":
                        if (this.insertPayload(payload)) {
                            System.out.println("insert successful");
                        }
                        else {
                            System.out.println("insert failed");
                        }
                        break;
                    case "delete":
                        if (this.controller.deleteCatalog(payload.get("id")).getStatus() == 200) {
                            System.out.println("delete successful");
                        }
                        else {
                            System.out.println("delete failed");
                        }
                        break;
                    case "update":
                        if (this.insertPayload(payload) &&
                                (this.controller.deleteCatalog(payload.get("id")).getStatus() == 200)) {
                            System.out.println("update successful");
                        }
                        else {
                            System.out.println("update failed");
                        }
                        break;
                }
            }
            catch (JsonProcessingException jsonParseException) {
                jsonParseException.printStackTrace();
            }
        }

        private void fillProperties(Map<String, String> p, String url)
        {
            String[] fields = url.split("\\?");
            p.put("connection-url", fields[0]);
            for (int i = 1; i < fields.length; i++) {
                String[] params = fields[i].split("=");
                switch (params[0]) {
                    case "user":
                        p.put("connection-user", params[1]);
                        break;
                    case "password":
                        p.put("connection-password", params[1]);
                        break;
                }
            }
        }

        private boolean insertPayload(Map<String, String> payload)
        {
            CatalogVo catalog = new CatalogVo();
            catalog.catalogName = payload.get("id");
            catalog.connectorName = "postgresql";
            catalog.properties = new HashMap<String, String>();
            this.fillProperties(catalog.properties, payload.get("url"));
            Response r = this.controller.addCatalog(catalog);
            return r.getStatus() == 200;
        }

        private boolean insertCatalog(String id, String connectionString)
        {
            System.out.println("id: " + id);
            System.out.println("connectionString: " + connectionString);
            CatalogVo catalog = new CatalogVo();
            catalog.catalogName = id;
            catalog.connectorName = "postgresql";
            catalog.properties = new HashMap<>();
            this.fillProperties(catalog.properties, connectionString);
//        catalog.properties.put("connection-url", connectionString);
            Response r = this.controller.addCatalog(catalog);
            return r.getStatus() == 200;
        }
    }
}
