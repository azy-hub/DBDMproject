package org.dant.rest;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.dant.model.Column;
import org.dant.select.SelectMethod;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestPath;

import java.util.List;
import java.util.concurrent.CompletionStage;

@Path("/slave")
@RegisterRestClient(configKey = "slave1-api")
public interface ForwardSlave1 extends ForwardSlave {
}