// see https://github.com/mu-semtech/mu-javascript-template for more info
import { app, query, update, errorHandler, uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDecimal, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import fs from 'fs';
import mime from 'mime';
import DOMPurify from 'dompurify';
import { JSDOM } from 'jsdom';
import { isAdminUser } from './lib/authorization';

const purify = DOMPurify( new JSDOM('').window );
const IGNORED_ORGANIZATIONS=["Pintafish (VLB)"];

// Assumptions
// - there is a single offering
// - all product information, except for isEnabled is under our control
// - product.id is the stable external identifier
// - all products which were retrieved earlier will be retrieved again (risky given we only fetch public products)

const PREFIXES = `
  PREFIX schema: <http://schema.org/>
  PREFIX veeakker: <http://veeakker.be/vocabularies/shop/>
  PREFIX food: <http://data.lirmm.fr/ontologies/food#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX gr: <http://purl.org/goodrelations/v1#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX prov: <http://www.w3.org/ns/prov#>
`;

const STORE_ID = 2927; // VT Boutersem
const PICKUP_POINT_ID = 563; // Pick up point VT Boutersem

function visibleProductsUrl(storeId, pickupPointId, page = 0, pageSize = 36) {
  return `https://api.localfoodworks.eu/api/store/${storeId}/visible-products?size=${pageSize}&sort=name,asc&pickUpPointId=${pickupPointId}&page=${page}`;
}

/**
 * Fetches and caches a JSON page
 *
 * @param {string} url Place to download the json body from.
 * @param {string} filePath Place to store the JSON file under `/share`.
 * @return {Object} Parsed JSON object.
 */
async function cachedJSONPage(url, filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath));
  } catch (e) {
    console.log(`Could not find file "${filePath}" for url "${url}" , fetching`);
    let response = await fetch(url);
    let jsonBody = await response.json();
    fs.writeFileSync(filePath, JSON.stringify(jsonBody));
    return jsonBody;
  }
}

/**
 * Fetches the page from disk if it exists or gets it from the backend and persists it to disk for later.
 * @return {Promise<Object>}
 */
async function ensurePage( pageNumber ) {
  const filePath = `/page-cache/${pageNumber}.json`;
  const url = visibleProductsUrl(STORE_ID, PICKUP_POINT_ID, pageNumber);

  return await cachedJSONPage(url, filePath);
}

/**
 * Fetches a detail page from disk if it exists or gets it from the backend and persists it to disk for later.
 * @param {string|number} productId
 * @param {string|number} shopId;
 * @return {Promise<ProductDetail>}
 */
async function ensureProductPage(storeId, productId) {
  const filePath = `/page-cache/product-${productId}.json`;
  const url = `https://api.localfoodworks.eu/api/store/${storeId}/products/${productId}`;

  return await cachedJSONPage(url, filePath);
}

/**
 * Fetches the overview of suppliers from disk if it exists or gets it from the backend and persists it to disk for
 * later use.
 * @param {string|number} storeId;
 * @return {Promise<[SupplierSummary]>} Parsed JSON page of suppliers.
 */
async function ensureSuppliersPage(storeId) {
  const filePath = `/page-cache/shop-${storeId}-suppliers.json`;
  const url = `https://api.localfoodworks.eu/api/store/${storeId}/suppliers`;

  return await cachedJSONPage(url, filePath);
}

/**
 * Downloads a file to be stored on a SHARE link in the files.
 *
 * @param {string} url Place to download the file from.
 * @param {string} filename Place to store the file.
 * @return {Promise}
 */
async function downloadShareFile( url, filename ) {
  const filePath = `/share/${filename}`;
  let response = await fetch(url);
  let arrayBuffer = await response.arrayBuffer();
  let buffer = Buffer.from(arrayBuffer);
  fs.writeFileSync(filePath, buffer);
  return;
}

/**
 * @typedef {Object} BaseProduct
 * @property {number} id Product identifier
 * @property {string} name Product name eg: 5 pannenkoeken
 * @property {string} supplier Supplier as string identifier eg: "Het Nijswolkje",
 * @property {number} numberOfUnitsOrdered": Unclear
 * @property {Object} pricing Describes ricing information
 * @property {Object} pricing.consumerPrice Describes the price for the consumer and its breakdown
 * @property {string} pricing.consumerPrice.type Only saw "UNIT" for now
 * @property {Object} pricing.consumerPrice.measurementUnitPrice Standard order price (eg: price per kg)
 * @property {Object} pricing.consumerPrice.measurementUnitPrice.money Effective price
 * @property {number} pricing.consumerPrice.measurementUnitPrice.money.amount Actual price number. eg: 10.37
 * @property {string} pricing.consumerPrice.measurementUnitPrice.money.currency Currency for the price. eg: EUR
 * @property {"stk"|"kg"} pricing.consumerPrice.measurementUnitPrice.unitOfMeasurement Per how much the price is.   'stk', 'kg', 'l' eg: "stk"
 * @property {Object} pricing.consumerPrice.orderUnitPrice Price for ordering a specific unit. (eg: price per kg)
 * @property {Object} pricing.consumerPrice.orderUnitPrice.money Effective price
 * @property {number} pricing.consumerPrice.orderUnitPrice.money.amount Actual price number. eg: 10.37
 * @property {string} pricing.consumerPrice.orderUnitPrice.money.currency Currency for the price. eg: EUR
 * @property {"stk"|"kg"} pricing.consumerPrice.orderUnitPrice.unitOfMeasurement Per how much the price is.  'stk', 'kg', 'l'. eg: "stk"
 * @property {Object} pricing.consumerPrice.breakdown Who earns what for this product, empty here.
 * @property {null} pricing.consumerPrice.consumerActionPrice Special action price, not used by us. eg: null
 * @property {number} pricing.measurementUnitVsOrderUnitRatio How much is one ordered unit versus the unit price. eg: 1
 * @property {boolean} canBeOrderedAsFractionOfOrderUnit Always false for us.  eg: false
 * @property {boolean} available Whether the product is available now (always true for us). eg: true
 * @property {string} latestOrderDate Up to when can the product be ordered, ignored by us. eg: "2025-04-20",
 * @property {string} earliestPickUpDate From when can the product be picked up, ignored by us. eg: "2025-04-23",
 * @property {boolean} bio Is this a bio product? eg: false,
 * @property {boolean} pgs I don't know.  eg: false,
 * @property {string} image Thumbnail image eg: "https://localfoodworks-images.s3-eu-west-1.amazonaws.com/products/181/c0339609-8fdc-4a1b-8703-945623b77837.png",
 * @property {string} content How much is in one package as text.  eg: "5",
 * @property {boolean} deliverable Can the product be delivered?  Always true for us. eg: true
 */

/**
 * @typedef {Object} SupplierSummary
 * @property {number} id Supplier identifier.
 * @property {string} name Name of the supplier as presented in the interface.
 */

/**
 * @typedef {string} SupplierName
 */

/**
 * @typedef {Object} SupplierInfo
 * @property {string} supplier.name Name of the supplier
 * @property {string} supplier.description Description of the supplier as formattet text (non html)
 * @property {string} supplier.emailAddress Email address of the supplier
 * @property {string} supplier.image Promotional picture belonging to the supplier
 */

/**
 * @typedef {BaseProduct & { supplier: SupplierName }} ProductListed
 * @typedef {BaseProduct & { supplier: SupplierInfo }} ProductDetail
 * @typedef {ProductListed | ProductDetail} Product
 */

/**
 * Ingests an individual product.
 * @param {ProductListed} product
 * @param {Object} options Options for fetching the product.
 * @param {boolean} options.external Fetch product externally through its custom paylod.  Defaults to false.
 * @param {string} options.job Job to which the product harvesting will be connected.
 */
async function loadProduct( product, options ) {
  // NOTE: we should search for the old information and keep its identifiers whenever possible.
  if ( options && options.external === true ) {
    product = await ensureProductPage(STORE_ID, product.id);
  }

  console.log(`Loading ${JSON.stringify(product)}`);
  let { productUri, admsIdentifier: _identifier } = await ensureProductMeta(product);
  if ( !IGNORED_ORGANIZATIONS.includes(product.supplier) ) {
    if( options.job )
      await ensureProductJobConnection(productUri, options.job);
    await ensureBaseProductInfo(product, productUri);
    await ensureProductDefaultPricing(product, productUri);
    const offeringResources = await ensureProductOffers(product, productUri);
    await ensureProductIngredients(product, productUri);
    await ensureProductAllergens(product, productUri);
    await ensureProductPicture(product, productUri);
    if (typeof product.supplier === "object") {
      // product has a supplier object so it has to be a ProductDetail
      await loadProductSupplier( offeringResources.offering, product.supplier );
    }
  }
}

/**
 * Connects the product to the job.
 * @param {string} productUri URI of the product to be connected.
 * @param {string} jobUri URI of the job to which the product should be connected.
 */
async function ensureProductJobConnection(productUri, jobUri) {
  await update(`
    ${PREFIXES}
    INSERT DATA {
      ${sparqlEscapeUri(productUri)} prov:wasGeneratedBy ${sparqlEscapeUri(jobUri)}
    }`);
}

/**
 * Stores the product's default pricing.
 * This should be roughly the price per unit (which may be KG).
 * @param {Product} product The product payload.
 * @param {string} productUri Internal identifier
 */
async function ensureProductDefaultPricing(product, productUri) {
  const consumerPrice = product.pricing.consumerPrice;
  const priceSpecification = consumerPrice.measurementUnitPrice;
  const euros = priceSpecification.money.amount;
  const unitOfMeasurement = priceSpecification.unitOfMeasurement;
  // It is less important to keep the uri for the TypeAndQuantityNode and the UnitPriceSpecification in this case but
  // we'll try to keep them as an exercise.

  const singleUnitPriceResource = await ensureSingleUnitPriceResource(productUri);
  const targetUnitResource = await ensureTargetUnitResource(productUri);

  await update(`
    ${PREFIXES}
    DELETE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(singleUnitPriceResource)} ?p ?o.
      }    
    } WHERE {
      GRAPH <http://mu.semte.ch/application> {
        VALUES ?p {
          gr:hasUnitOfMeasurement gr:hascurrencyValue
        }
        ${sparqlEscapeUri(singleUnitPriceResource)} ?p ?o.
      }
    };
    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        # All the information we can find.
        ${sparqlEscapeUri(singleUnitPriceResource)}
          gr:hasUnitOfMeasurement ${sparqlEscapeString(convertLfwUnitToCEFACT(unitOfMeasurement))};
          gr:hasCurrencyValue ${sparqlEscapeDecimal(euros)}.
      }
    }`);

  const targetUnitMeasurementUnit = convertLfwUnitToCEFACT(consumerPrice.measurementUnitPrice.unitOfMeasurement);
  const targetUnitMeasurementAmount = product.pricing.measurementUnitVsOrderUnitRatio;

  await update(`
    ${PREFIXES}
    DELETE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(targetUnitResource)} ?p ?o.
      }
    } WHERE {
      GRAPH <http://mu.semte.ch/application> {
        VALUES ?p {
          gr:hasUnitOfMeasurement gr:hasValue
        }
        ${sparqlEscapeUri(targetUnitResource)} ?p ?o.
      }
    };
    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        # All the information we can find.
        ${sparqlEscapeUri(targetUnitResource)}
          gr:hasUnitOfMeasurement ${sparqlEscapeString(targetUnitMeasurementUnit)};
          gr:hasValue ${sparqlEscapeDecimal(targetUnitMeasurementAmount)}.
      }
    }`);
}

/**
 * Convert an LFW unit to a CEFACT unit.
 * @param {string} unit Unit to be converted.
 * @return {string} CEFACT unit as used in webshop.
 */
function convertLfwUnitToCEFACT(unit) {
  switch (unit) {
    case "l":
      return "LTR";
    case "kg":
      return "KGM";
    case "stk":
      return "C62";
    default:
      throw `Could not translate unit ${unit}`;
  }
}

/**
 * Ensures a UnitPriceSpecification resource exists for productUri.
 * @param {string} productUri
 * @return {string} Uri of the relationship to the veeakker:singleUnitPrice.
 */
async function ensureSingleUnitPriceResource( productUri ) {
  const currentBindings = (await query(`${PREFIXES}
    SELECT ?unitPriceSpecification
    WHERE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)} veeakker:singleUnitPrice ?unitPriceSpecification.
      }
    } LIMIT 1`)).results.bindings;
  if ( currentBindings.length > 0 ) {
    return currentBindings[0].unitPriceSpecification.value;
  } else {
    const unitPriceSpecificationUuid = uuid();
    const unitPriceSpecificationUri = `http://veeakker.be/price-specifications/${unitPriceSpecificationUuid}`;
    await update(`${PREFIXES}
      INSERT DATA {
        GRAPH <http://mu.semte.ch/application> {
          ${sparqlEscapeUri(productUri)} 
            veeakker:singleUnitPrice
              ${sparqlEscapeUri(unitPriceSpecificationUri)}.
          ${sparqlEscapeUri(unitPriceSpecificationUri)}
            a gr:UnitPriceSpecification;
            mu:uuid ${sparqlEscapeString(unitPriceSpecificationUuid)}.
        }
      }`);
    return unitPriceSpecificationUri;
  }
}
/**
 * Ensures a QuantitativeValue resource exists for targetUnit.
 * @param {string} productUri
 * @return {string} Uri of the relationship to the veeakker:targetUnit.
 */
async function ensureTargetUnitResource( productUri ) {
  const currentBindings = (await query(`${PREFIXES}
    SELECT ?quantitativeValue
    WHERE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)} veeakker:targetUnit ?quantitativeValue.
      }
    } LIMIT 1`)).results.bindings;
  if ( currentBindings.length > 0 ) {
    return currentBindings[0].quantitativeValue.value;
  } else {
    const quantitativeValueUuid = uuid();
    const quantitativeValueUri = `http://veeakker.be/quantitative-values/${quantitativeValueUuid}`;
    await update(`${PREFIXES}
      INSERT DATA {
        GRAPH <http://mu.semte.ch/application> {
          ${sparqlEscapeUri(productUri)} 
            veeakker:targetUnit
              ${sparqlEscapeUri(quantitativeValueUri)}.
          ${sparqlEscapeUri(quantitativeValueUri)}
            a gr:QuantitativeValue;
            mu:uuid ${sparqlEscapeString(quantitativeValueUuid)}.
        }
      }`);
    return quantitativeValueUri;
  }
}

/**
 * Stores the product's pricing.
 * @param {Product} product The product payload.
 * @param {string} productUri Internal identifier
 * @return {Promise<OfferingResources>}
 */
async function ensureProductOffers(product, productUri) {
  // We assume there's one product offering
  const offering = await ensureOfferingResources(productUri);
  const euros = product.pricing.consumerPrice.orderUnitPrice.money.amount;
  await update(`${PREFIXES}
    DELETE {
      ${sparqlEscapeUri(offering.unitPrice)} ?p ?o.
    } WHERE {
      VALUES ?p { gr:hasUnitOfMeasurement gr:hasCurrencyValue }
      ${sparqlEscapeUri(offering.unitPrice)} ?p ?o.
    };
    INSERT DATA {
      ${sparqlEscapeUri(offering.unitPrice)}
        gr:hasUnitOfMeasurement "C62";
        gr:hasCurrencyValue ${sparqlEscapeDecimal(euros)}.
    }
  `);

  // When looking at this it turns out there is limited reasoning to be made,
  // the orderUnit is not very relevant for our case.  Even when a "piece" is
  // ordered, this boils down to one time the unit weight.

  const amount = product.pricing.measurementUnitVsOrderUnitRatio;
  const unit = convertLfwUnitToCEFACT(product.pricing.consumerPrice.measurementUnitPrice.unitOfMeasurement);

  await update(`${PREFIXES}
    DELETE {
      ${sparqlEscapeUri(offering.typeAndQuantity)} ?p ?o.
    } WHERE {
      VALUES ?p { gr:amountOfThisGood gr:hasUnitOfMeasurement gr:typeOfGood }
      ${sparqlEscapeUri(offering.typeAndQuantity)} ?p ?o.
    };
    INSERT DATA {
      ${sparqlEscapeUri(offering.typeAndQuantity)}
        gr:amountOfThisGood ${sparqlEscapeDecimal(amount)};
        gr:hasUnitOfMeasurement ${sparqlEscapeString(unit)}
    }`);
  return offering;
}

/**
 * @typedef OfferingResources
 * @property {string} offering Resource of the offering.
 * @property {string} typeAndQuantity Resource of the type and quantity.
 * @property {string} unitPrice Resource of the unit price specification.
 */

/**
 * Ensures there's a complete offering resource available.
 * @return {OfferingResources} resources Resulting entities
 */
async function ensureOfferingResources(productUri) {
  // ensure offering URI exists
  const offering = await ensureOffering(productUri);
  const typeAndQuantity = await ensureOfferingTypeAndQuantity(offering);
  const unitPrice = await ensureOfferingUnitPrice(offering);

  return {
    offering,
    typeAndQuantity,
    unitPrice 
  };
}

/**
 * Ensures the offering resource exists.
 * @param {string} productUri Uri of the product.
 * @return {string} URI of the offering.
 */
async function ensureOffering(productUri) {
  const bindings = (await query(`${PREFIXES}
    SELECT ?offeringUri WHERE {
      ${sparqlEscapeUri(productUri)} veeakker:offerings ?offeringUri.
    } LIMIT 1`)).results.bindings; // assume single offering at this point
  
  if (bindings.length > 0) {
    return bindings[0].offeringUri.value;
  } else {
    const offeringUuid = uuid();
    const offeringUri = `http://veeakker.be/offerings/${offeringUuid}`;

    await update(`${PREFIXES}
      INSERT DATA {
        ${sparqlEscapeUri(productUri)} 
          veeakker:offerings ${sparqlEscapeUri(offeringUri)}.
        ${sparqlEscapeUri(offeringUri)}
          a gr:Offering;
          mu:uuid ${sparqlEscapeString(offeringUuid)}.
          }`);

    return offeringUri;
  }
}

/**
 * Ensures the Offering's typeAndQuantity exists.
 * @param {string} offeringUri
 * @return {string} typeAndQuantity resource.
 */
async function ensureOfferingTypeAndQuantity(offeringUri) {
  const bindings = (await query(`${PREFIXES}
    SELECT ?typeAndQuantityUri
    WHERE {
      ${sparqlEscapeUri(offeringUri)} gr:includesObject ?typeAndQuantityUri.
    } LIMIT 1`)).results.bindings;
  if ( bindings.length > 0 ) {
    return bindings[0].typeAndQuantityUri.value;
  } else {
    const typeAndQuantityUuid = uuid();
    const typeAndQuantityUri = `http://veeakker.be/type-and-quantities/${typeAndQuantityUuid}`;

    await update(`${PREFIXES}
      INSERT DATA {
        ${sparqlEscapeUri(offeringUri)} 
          gr:includesObject ${sparqlEscapeUri(typeAndQuantityUri)}.
        ${sparqlEscapeUri(typeAndQuantityUri)}
          a gr:TypeAndQuantityNode;
          mu:uuid ${sparqlEscapeString(typeAndQuantityUuid)}.
      }`);

    return typeAndQuantityUri;
  }
}

/**
 * Ensures the Offering's unitPrice exists.
 * @param {string} offeringUri
 * @return {string} unitPrice resource.
 */
async function ensureOfferingUnitPrice(offeringUri) {
  const bindings = (await query(`${PREFIXES}
    SELECT ?unitPriceUri
    WHERE {
      ${sparqlEscapeUri(offeringUri)} gr:hasPriceSpecification ?unitPriceUri.
    } LIMIT 1`)).results.bindings;
  if ( bindings.length > 0 ) {
    return bindings[0].unitPriceUri.value;
  } else {
    const unitPriceSpecificationUuid = uuid();
    const unitPriceSpecificationUri = `http://veeakker.be/price-specifications/${unitPriceSpecificationUuid}`;

    await update(`${PREFIXES}
      INSERT DATA {
        ${sparqlEscapeUri(offeringUri)} 
          gr:hasPriceSpecification ${sparqlEscapeUri(unitPriceSpecificationUri)}.
        ${sparqlEscapeUri(unitPriceSpecificationUri)}
          a gr:UnitPriceSpecification;
          mu:uuid ${sparqlEscapeString(unitPriceSpecificationUuid)}.
      }`);

    return unitPriceSpecificationUri;
  }
}

/**
 * Ingests the product ingredients if they're in the payload.
 * If they are not in the payload, they are removed.
 * @param {Product} product The product payload.
 * @param {string} productUri The product's URI.
 */
async function ensureProductIngredients(product, productUri) {
  let sortedIngredientsList = product.ingredients
    ? product
      .ingredients
      .sort((a,b) => a.position - b.position)
      .map(({name}) => name)
      .map((name) => purify.sanitize(name, { USE_PROFILES: { html: true } }))
    : null;

  let ingredientsString = 
    product.ingredients
      // TODO: perform HTML escaping
      ? `<ul>${sortedIngredientsList.map((s) => `\n  <li>${s}</li>`).join("")}\n</ul>`
      : null;

  await update(`${PREFIXES}
    DELETE WHERE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)}
          food:ingredientListAsText ?oldIngredients.
      }
    }
    ${ ingredientsString ?
    `;
    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)}
          food:ingredientListAsText
            ${sparqlEscapeString(ingredientsString)}.
      }
    }` : ""
    }`);
}

/**
 * Ingests the product allergens if they're in the payload.
 * If they are not in the payload, they are removed.
 * @param {Product} product The product payload.
 * @param {string} productUri The product's URI.
 */
async function ensureProductAllergens(product, productUri) {
  let sortedAllergensList = product.allergens
    ? product
      .allergens
      .map(({allergen}) => allergen)
      .sort((a,b) => a.id - b.id)
      .map(({name}) => name)
      .map((name) => purify.sanitize(name, { USE_PROFILES: { html: true } }))
    : null;

  let allergensString =
    product.allergens
      ? `<ul>${sortedAllergensList.map((s) => `\n  <li>${s}</li>`).join("")}\n</ul>`
      : null;

  await update(`${PREFIXES}
    DELETE WHERE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)}
          veeakker:allergensAsText ?oldAllergens.
      }
    }
    ${ allergensString ?
    `;
    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)}
          veeakker:allergensAsText
            ${sparqlEscapeString(allergensString)}.
      }
    }` : ""
    }`);
}


/**
 * Ingests the product picture if it does not exist yet or if it is forced.
 * If the picture does not exist anymore, it is removed.
 *
 * The picture is downloaded when it has a different URI from the picture we have today.
 * 
 * @param {Product} product The product payload.
 * @param {string} productUri The product's URI.
 */
async function ensureProductPicture(product, productUri) {
  const currentPictureIsCorrect =
    product.image 
    && (await query(`${PREFIXES}
      ASK WHERE {
        ${sparqlEscapeUri(productUri)} veeakker:thumbnail ?picture.
        ?picture dct:source ${sparqlEscapeUri(product.image)}.
        }`)).boolean;

  if (currentPictureIsCorrect) {
    return true;
  } else {
    // Remove the old picture
    await update(`${PREFIXES}
      DELETE {
        ?share ?p ?o.
      } WHERE {
        VALUES ?p {
          dct:source
          nfo:fileName dct:format nfo:fileSize dbpedia:fileExtension dct:created
        }
        ${sparqlEscapeUri(productUri)} veeakker:thumbnail ?thumbnail.
        ?share nie:dataSource ?thumbnail.
        ?share ?p ?o.
      };
      DELETE {
        ?file ?p ?o.
      } WHERE {
        VALUES ?p {
          nfo:fileName dct:format nfo:fileSize dbpedia:fileExtension dct:created
        }
        ${sparqlEscapeUri(productUri)} veeakker:thumbnail ?s.
        ?s ?p ?o.
      };
      DELETE WHERE {
        ${sparqlEscapeUri(productUri)} veeakker:thumbnail ?thumbnail.
      }`);

    if ( product.image ) {
      // Add the new image
      const extension = product.image.match(/[^.]+$/)[0];
      const fileName = product.image.match(/[^/>]+$/)[0];

      // Download the new picture on a new share link
      const shareResourceUuid = uuid();
      const shareFileName = `${shareResourceUuid}.${extension}`;
      const shareResourceUri = `share://${shareFileName}`;
    
      const fileResourceUuid = uuid();
      const fileResourceUri = `http://veeakker.be/files/${shareResourceUuid}`;

      const creation = new Date();

      await downloadShareFile(product.image, shareFileName);
      const mimetype = mime.getType(`/share/${shareFileName}`);

      const response = await update(`${PREFIXES}
        INSERT DATA {
          ${sparqlEscapeUri(productUri)}
            veeakker:thumbnail ${sparqlEscapeUri(fileResourceUri)}.
          ${sparqlEscapeUri(fileResourceUri)}
            a nfo:FileDataObject;
            dct:source ${sparqlEscapeUri(product.image)};
            mu:uuid ${sparqlEscapeString(fileResourceUuid)};
            nfo:fileName ${sparqlEscapeString(fileName)};
            dct:format ${sparqlEscapeString(mimetype)};
            # nfo:fileSize
            dbpedia:fileExtension ${sparqlEscapeString(extension)};
            dct:created ${sparqlEscapeDateTime(creation)}.
          ${sparqlEscapeUri(shareResourceUri)}
            a nfo:FileDataObject;
            mu:uuid ${sparqlEscapeString(shareResourceUuid)};
            nie:dataSource ${sparqlEscapeUri(fileResourceUri)};
            nfo:fileName ${sparqlEscapeString(fileName)};
            dct:format ${sparqlEscapeString(mimetype)};
            # nfo:fileSize
            dbpedia:fileExtension ${sparqlEscapeString(extension)};
            dct:created ${sparqlEscapeDateTime(creation)}.
        }
      `);
    }
  }
}

/**
 * Ensures metadata about the product exists.
 * @param {Product} product
 * @param {string|undefined} jobUri
 */
async function ensureProductMeta( product, jobUri ) {
  let myQuery = `${PREFIXES}

    SELECT ?productUri ?admsIdentifier
    WHERE {
      ?productUri
        a schema:Product;
        adms:identifier ?admsIdentifier.
      ?admsIdentifier
        a adms:Identifier;
        skos:notation ${sparqlEscapeString(""+product.id)};
        dct:creator <https://localfoodworks.eu/>.
    }`;
  console.log(myQuery);
  const bindings = (await query(myQuery)).results.bindings;
  if(bindings.length) {
    console.log(`Found ${JSON.stringify(bindings[0])}`);
    return {
      productUri: bindings[0].productUri.value,
      admsIdentifier: bindings[0].admsIdentifier.value
    }
  } else {
    const productUuid = uuid();
    const productUri = `http://veeakker.be/products/${productUuid}`;

    const identifierUuid = uuid();
    const admsIdentifier = `http://data.redpencil.io/identifiers/${identifierUuid}`;

    await update(`${PREFIXES}

    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)}
          a schema:Product;
          mu:uuid ${sparqlEscapeString(productUuid)};
          adms:identifier ${sparqlEscapeUri(admsIdentifier)}.
        ${sparqlEscapeUri(admsIdentifier)}
          a adms:Identifier;
          mu:uuid ${sparqlEscapeString(identifierUuid)};
          skos:notation ${sparqlEscapeString("" + product.id)};
          dct:creator <https://localfoodworks.eu/>;
          dct:title ${sparqlEscapeString("LFW " + product.id)}.
      }
    }`);

    console.log(`Created ${productUri} with ${admsIdentifier}`);

    return {
      productUri,
      admsIdentifier
    }
  }
}

/**
 * Ensures basic product information is known.
 * @param {Product} product
 * @param {string} productUri
 */
 async function ensureBaseProductInfo(product, productUri) {
  await update(`
    ${PREFIXES}
    DELETE {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(productUri)} ?p ?o.
      }    
    } WHERE {
      GRAPH <http://mu.semte.ch/application> {
        VALUES ?p {
          dct:title dct:description veeakker:hasLabel veeakker:plu veeakker:sortIndex veeakker:lfwProductCanBeOrderedByFractionOfOrderUnit
        }
        ${sparqlEscapeUri(productUri)} ?p ?o.
      }
    };
    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        # All the information we can find.
        ${sparqlEscapeUri(productUri)}
          dct:title ${sparqlEscapeString(product.name || "")};
          # TODO: for product.description we need to fetch the product detail instead
          ${product.description ? `dct:description ${sparqlEscapeString(product.description)};` : ""}
          veeakker:hasLabel <http://veeakker.be/labels/lfw>;
          ${product.bio ? `veeakker:hasLabel <http://veeakker.be/labels/bio>;` : ""}
          # veeaker:isPublic will just not be set
          veeakker:plu ${sparqlEscapeDecimal(1000000 + product.id)};
          veeakker:sortIndex ${sparqlEscapeDecimal(1000000 + product.id)};
          veeakker:lfwProductCanBeOrderedByFractionOfOrderUnit ${sparqlEscapeBool(product.canBeOrderedAsFractionOfOrderUnit)}.
      }
    }`);
}

/**
  * @param {Array<Product>} products
 */
function printBasicPricingInfo(products) {
  console.table(products.map( (product) => {
    return {
      measurementUnit:
        product.pricing.consumerPrice.measurementUnitPrice.unitOfMeasurement,
      orderUnit:
        product.pricing.consumerPrice.orderUnitPrice.unitOfMeasurement,
      factor:
        product.pricing.measurementUnitVsOrderUnitRatio
    };
  }));    
}

/**
 * Constructs a new load job which can be used to link products to the job(s) that fetched them.
 * @return {Promise<string>} URI of this job.
 */
async function createLoadJob() {
  const lfwFetchJobUuid = uuid();
  const lfwFetchJobUri = `http://veeakker.be/lfw-jobs/${lfwFetchJobUuid}`;

  // TODO: veeakker:LfwFetchJob should subclass cogs:Job.

  await update(`
    ${PREFIXES}
    INSERT DATA {
      GRAPH <http://mu.semte.ch/application> {
        ${sparqlEscapeUri(lfwFetchJobUri)}
          a veeakker:LfwFetchJob;
          mu:uuid ${sparqlEscapeString(lfwFetchJobUuid)};
          dct:created ${sparqlEscapeDateTime(new Date())}.
      }
          }`);

  return lfwFetchJobUri;
}

/**
 * Starts the job.
 * @param {string} jobUri Job identifier to start.
 */
async function startJob(jobUri) {
  await update(`
    ${PREFIXES}
    DELETE WHERE {
      ${sparqlEscapeUri(jobUri)} adms:status ?status.
    };
    INSERT DATA {
      ${sparqlEscapeUri(jobUri)} adms:status <http://veeakker.be/lfw-job-statusses/running>.
    }`);
}

/**
 * Stops the job.
 * @param {string} jobUri Job identifier to stop.
 */
async function finishJob(jobUri) {
  await update(`
    ${PREFIXES}
    DELETE WHERE {
      ${sparqlEscapeUri(jobUri)} adms:status ?status.
    };
    INSERT DATA {
      ${sparqlEscapeUri(jobUri)} adms:status <http://veeakker.be/lfw-job-statusses/finished>.
    }`);
}

/**
 * Errors the job.
 * @param {string} jobUri Job identifier to error.
 */
async function errorJob(jobUri) {
  // TODO: allow comment for failed job.
  await update(`
    ${PREFIXES}
    DELETE WHERE {
      ${sparqlEscapeUri(jobUri)} adms:status ?status.
    };
    INSERT DATA {
      ${sparqlEscapeUri(jobUri)} adms:status <http://veeakker.be/lfw-job-statusses/error>.
    }`);
}

/**
 * Loads the suppliers by fetching the relevant document and updating their core information.
 *
 * This may, in the future, also fetch information from the suppliers' detail page.
 */
async function loadSuppliers(jobUri) {
  const suppliers = await ensureSuppliersPage(STORE_ID);

  // We want to keep the URIs of the entities which already exist, hence we first ensure we have an object for each ID,
  // then we start filling in the details.
  for( const supplier of suppliers ) {
    const hasSupplier = (await query(`${PREFIXES}
    ASK {
      ?fneBusiness a gr:BusinessEntity;
        adms:identifier ?fneIdentifier.
      ?fneIdentifier dct:creator <https://localfoodworks.eu/>;
        skos:notation ${sparqlEscapeString(`${supplier.id}`)}.
    }`)).boolean;
    if( !hasSupplier ) {
      const supplierUuid = uuid();
      const supplierUri = `http://veeakker.be/suppliers/${supplierUuid}`;
      const identifierUuid = uuid();
      const identifierUri = `http://data.redpencil.io/identifiers/${identifierUuid}`;

      await update(`${PREFIXES}
      INSERT DATA {
        ${sparqlEscapeUri(supplierUri)}
          a gr:BusinessEntity;
          mu:uuid ${sparqlEscapeString(supplierUuid)};
          adms:identifier ${sparqlEscapeUri(identifierUri)}.
        ${sparqlEscapeUri(identifierUri)}
          a adms:Identifier;
          mu:uuid ${sparqlEscapeString(identifierUuid)};
          skos:notation ${sparqlEscapeString(`${supplier.id}`)};
          dct:creator <https://localfoodworks.eu/>;
          dct:title ${sparqlEscapeString(`LFW Supplier ID ${supplier.id}`)}.
          }`);
    }
  }

  // Now we know all entities have the desired identifier and we can set other properties fetched.  Today that's only the name.

  await update(`${PREFIXES}
  DELETE {
    ?supplier gr:name ?oldName.
  } INSERT {
    ?supplier gr:name ?newName.
  } WHERE {
  VALUES ( ?externalIdentifier ?newName ) {
    ${suppliers.map( (supplier) => {
      return `( ${sparqlEscapeString(`${supplier.id}`)} ${sparqlEscapeString(supplier.name)})`
    }).join(`\n    `)}
    }
    ?supplier a gr:BusinessEntity;
      adms:identifier ?identifier.
    ?identifier dct:creator <https://localfoodworks.eu/>;
      skos:notation ?externalIdentifier.

    OPTIONAL { ?supplier gr:name ?oldName }
  }`);
}

/**
 * Loads the supplier information for a product, assuming the supplier is already in the database.
 * @param {string} offeringUri Product for which the supplier was found.
 * @param {SupplierInfo} supplier Detailed information of the supplier.
 * @return {Promise}
 */
async function loadProductSupplier(offeringUri, supplier) {
  console.log(`Loading supplier for ${offeringUri}`);

  // Find the identifier of the supplier
  const supplierInfos = (await query(`${PREFIXES}
  SELECT ?supplier
  WHERE {
    ?supplier gr:name ${sparqlEscapeString(supplier.name)}.
    }`)).results.bindings;
  const supplierUri = supplierInfos.length ? supplierInfos[0].supplier.value : null;
  const description = purify.sanitize(
    (supplier.description || "").replaceAll("\n", "<br />"),
    { USE_PROFILES: { html: true } }
  );

  // Update information of the supplier
  if( supplierUri ) {
    await update(`${PREFIXES}
    DELETE {
      ${sparqlEscapeUri(supplierUri)} ?p ?o.
    } WHERE {
      VALUES ?p { schema:email dct:description }
      ${sparqlEscapeUri(supplierUri)} ?p ?o.
    };
    INSERT DATA {
      ${sparqlEscapeUri(supplierUri)} schema:email ${sparqlEscapeString(supplier.emailAddress)};
        dct:description ${sparqlEscapeString(description)};
        gr:offers ${sparqlEscapeUri(offeringUri)}.
    }`)
  }

  // TODO: Attach the picture to the supplier
}

/**
  * Loads the pages by walking over each page number.
 */
async function loadPages(jobUri) {
  let counter = 0;
  let page;
  do {
    page = await ensurePage( counter );
    console.log(`LOADING PAGE ${counter}`);
    printBasicPricingInfo(page.content);
    for ( const product of page.content ) {
      console.log(`LOADING PRODUCT`);
      console.log(JSON.stringify(product));
      console.log(product);
      await loadProduct(product, { job: jobUri, external: true });
    }
    counter++;
  } while (page.last == false)

  console.log(`ENDED WITH PAGE ${counter}`);
}

const LOAD_PAGES_ON_STARTUP = false;  // NOTE: this may require updated acces rights

setTimeout( async () => {
  // downloadShareFile("https://veeakker.be/", "veeakker-index.html");

  // A. load one product
  // const page = await ensurePage( 0 );
  // await loadProduct(page.content[0], { external: true });
  // console.log(page.content[0]);
  // console.log(JSON.stringify(page.content[0]));

  // B. load all pages
  const loadAllPages = LOAD_PAGES_ON_STARTUP;
  if (loadAllPages) {
    const jobUri = await createLoadJob();
    try {
      await startJob(jobUri);
      await loadSuppliers(jobUri);
      await loadPages(jobUri);
      await finishJob(jobUri);
    } catch (e) {
      await errorJob(jobUri);
    }
  }
  // C. debugging information
  // for ( const product of page.content ) {
  //   console.log(`LOADING PRODUCT`);
  //   console.log(JSON.stringify(product));
  //   console.log(product);
  //   await loadProduct(product);
  // }
  // console.log("Loaded product");
  // console.log(
  //   [... new Set(
  //     page
  //       .content
  //       .map( (product) => product.pricing.consumerPrice.measurementUnitPrice.unitOfMeasurement ))]
  // );
}, 2000);

/**
 * Initiates a harvesting process.
 */
app.post('/harvest', async function(req, res) {
  if( await isAdminUser(req) ) {
    const jobUri = await createLoadJob();
    try {
      await startJob(jobUri);
      await loadSuppliers(jobUri);
      await loadPages(jobUri);
      await finishJob(jobUri);
      res.status(200).send("Harvest successful");
    } catch (e) {
      console.error(`${e}`);
      await errorJob(jobUri);
      res.status(500).send("Failed to harvest");
    }
  } else {
    res.status(500).send("Missing access rights");
  }
});

app.get('/', function (req, res) {
  res.send('Hello mu-javascript-template');
});

app.use(errorHandler);
