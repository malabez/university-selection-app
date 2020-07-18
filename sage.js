//= require sage/axios/dist/axios.standalone.js
//= require sage/CryptoJS/rollups/hmac-sha256.js
//= require sage/CryptoJS/rollups/sha256.js
//= require sage/CryptoJS/components/hmac.js
//= require sage/CryptoJS/components/enc-base64.js
//= require sage/url-template/url-template.js
//= require sage/apiGatewayCore/sigV4Client.js
//= require sage/apiGatewayCore/apiGatewayClient.js
//= require sage/apiGatewayCore/simpleHttpClient.js
//= require sage/apiGatewayCore/utils.js
//= require apigClient.js


function sage(gre){
  var univ = {"Alabama A & M University": 1, "Alabama State University": 1, "Amridge University": 3, "Auburn University": 2, "Auburn University at Montgomery": 1, "Columbia Southern University": 3, "Faulkner University": 4, "Heritage Christian University": 1, "Jacksonville State University": 3, "Oakwood University": 3, "Samford University": 1, "School Of Advanced Air And Space Studies": 1, "South University Montgomery": 3, "Spring Hill College": 4, "Troy University": 2, "Tuskegee University": 2, "United States Sports Academy": 4, "The University of Alabama": 4, "University of Alabama at Birmingham": 3, "University of Alabama in Huntsville": 2, "University of Mobile": 4, "University of Montevallo": 1, "University of North Alabama": 3, "University Of Phoenix–Birmingham Campus": 1, "University of South Alabama": 4, "University of West Alabama": 3, "Virginia College-Birmingham": 4, "George C Wallace State Community College-Hanceville": 2, "Alaska Pacific University": 4, "Alaska University Of Southeast": 4, "University of Alaska Fairbanks": 3, "University of Alaska Anchorage": 4, "Acacia University": 3, "Advancing Technology University Of": 3, "Argosy University-Phoenix": 4, "Arizona School Of Acupuncture And Oriental Medicine": 3, "Arizona State University-Tempe": 3, "De Vry University Glendale": 4, "DeVry University-Arizona": 3}
  var apigClient = apigClientFactory.newClient({
    accessKey: "",
    secretKey: "",
    region: "us-east-1"
  });
  var params = {};
  a = $("#sage_value").val();
  b = univ[$("#sage_value").val()];
  c = $("#gpa_value").val();
  d = gre;
  var body = {
    name: a,
    data: d+","+c+","+b
  };
  var additionalParams = {};

  apigClient
    .sagePost(params, body, additionalParams)
    .then(function(result) {
      // alert(result.data.body);
      $("#result").append(result.data);
      debugger;
      console.log(result.data);
    })
    .catch(function(result) {
      // alert(result.message);
      debugger;
      $("#result").append(result.message);
      console.log(result.message);
    });
  
};