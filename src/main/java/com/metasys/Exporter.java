package com.metasys;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.impl.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by kbryd on 1/16/15.
 */
public class Exporter {

    private static Logger logger = Logger.getLogger(Exporter.class.getName());

    public static void exportSingleDocument(Session session, Document document, String destinationPath) throws IOException {
        String initialSep = "";

        if (!destinationPath.endsWith(File.separator)) {
            initialSep = File.separator;
        }
        String sep = File.separator;

        ContentStream contentStream = document.getContentStream();
        final List<Property<?>> properties = document.getProperties();


        for (String path : document.getPaths()) {
            path = path.substring(0, path.lastIndexOf('/'));

            String exportPath = destinationPath + initialSep + path + sep + document.getName();
            String exportMetadataPath = destinationPath + initialSep + path + sep + document.getName() + "_metadata.xml";
            File file = new File(exportPath);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            if (!file.exists()) {
                logger.log(Level.INFO, String.format("Creating empty document '%s' from '%s' to '%s'", document.getName(), path, exportPath));
                file.createNewFile();
            }
            if (contentStream != null) {
                logger.log(Level.INFO, String.format("Exporting document '%s' (%d bytes) from '%s' to '%s'", document.getName(), document.getContentStreamLength(), path, exportPath));

                try (InputStream inStream = contentStream.getStream(); FileOutputStream outStream = new FileOutputStream(exportPath)) {
                    IOUtils.copy(inStream, outStream);
                }

            }

            if (properties != null && !properties.isEmpty()) {
                String metadataFileName = document.getName() + "_metadata.xml";
                logger.log(Level.INFO, String.format("Exporting metadata '%s' from '%s' to '%s'", metadataFileName, path, exportMetadataPath));

                file = new File(exportMetadataPath);
                file.createNewFile();

                StringBuilder sb = new StringBuilder();
                sb.append("<?xml>\n");
                sb.append("<metadata>\n");
                sb.append("<sourcePath>" + path + "</sourcePath>\n");
                for (Property property : properties) {
                    String qName = property.getQueryName();
                    String value = property.getValuesAsString();
                    if (property.getValue() == null) {
                        continue;
                    }
                    sb.append("<" + qName + ">" + value + "</" + qName + ">\n");
                }
                sb.append("</metadata>\n");
                Files.writeString(file.toPath(), sb);
            }
        }

    }

    public static void exportSingleFolder(Folder folder, String destinationPath) {
        String sep = "";

        if (!destinationPath.endsWith(File.separator)) {
            sep = File.separator;
        }

        String exportPath = destinationPath + sep + folder.getPath();
        logger.log(Level.INFO, String.format("Exporting folder '%s' from '%s' to '%s'", folder.getName(), folder.getPath(), exportPath));

        File file = new File(exportPath);
        file.mkdirs();
    }


    public static void export(CMISSession cmisSession, String startingPath, String destinationPath, Integer maxLevels) throws IOException {
        Session session = cmisSession.getSession();

        ObjectType type = session.getTypeDefinition("cmis:document");
        PropertyDefinition<?> objectIdPropDef = type.getPropertyDefinitions().get(PropertyIds.OBJECT_ID);
        String objectIdQueryName = objectIdPropDef.getQueryName();
        Folder startingFolder = (Folder) session.getObjectByPath(startingPath);


        exportFolders(destinationPath, session, objectIdQueryName, startingFolder);

        exportDocuments(destinationPath, session, objectIdQueryName, startingFolder);
    }

    private static void exportFolders(String destinationPath, Session session, String objectIdQueryName, Folder startingFolder) {
        String queryString = "SELECT " + objectIdQueryName + " FROM cmis:folder F WHERE IN_TREE(F, '" + startingFolder.getId() + "')";
        ItemIterable<QueryResult> results = session.query(queryString, false);

        for (QueryResult qResult : results) {
            String objectId = qResult.getPropertyValueByQueryName("F.cmis:objectId");
            Folder folder = (Folder) session.getObject(session.createObjectId(objectId));

            exportSingleFolder(folder, destinationPath);
        }
    }

    private static void exportDocuments(String destinationPath, Session session, String objectIdQueryName, Folder startingFolder) throws IOException {
        String queryString = "SELECT " + objectIdQueryName + " FROM cmis:document F WHERE IN_TREE(F, '" + startingFolder.getId() + "')";
        ItemIterable<QueryResult> results = session.query(queryString, false);

        for (QueryResult qResult : results) {
            String objectId = qResult.getPropertyValueByQueryName("F.cmis:objectId");
            Document doc = (Document) session.getObject(session.createObjectId(objectId));

            exportSingleDocument(session, doc, destinationPath);
        }
    }
}
