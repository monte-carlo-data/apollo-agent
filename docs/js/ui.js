window.onload = () => {
window.ui = SwaggerUIBundle({
  url: 'swagger.json',
  dom_id: '#swagger-ui',
  supportedSubmitMethods: [],  //disable try-it-out feature
});
};
