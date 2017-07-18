package com.sagittarius.example;

import com.datastax.driver.core.*;

/**
 * Created by Leah on 2017/7/11.
 */
public class ReadClusterState {
    static String[] CONTACT_POINTS = {"192.168.15.114"};
    static int PORT = 9042;

    public static void main(String[] args) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoints(CONTACT_POINTS).withPort(PORT)
                    .build();

            Metadata metadata = cluster.getMetadata();
            System.out.printf("Connected to cluster: %s%n", metadata.getClusterName());

            for (Host host : metadata.getAllHosts()) {
                System.out.printf("Datatacenter: %s; Host: %s; Rack: %s%n, State:%s, Token:%s, Load:%s",
                        host.getDatacenter(), host.getAddress(), host.getRack(), host.isUp(), host.getTokens().size(), host.getState());
            }

            for (KeyspaceMetadata keyspace : metadata.getKeyspaces()) {
                for (TableMetadata table : keyspace.getTables()) {
                    System.out.printf("Keyspace: %s; Table: %s%n",
                            keyspace.getName(), table.getName());
                }
            }

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
