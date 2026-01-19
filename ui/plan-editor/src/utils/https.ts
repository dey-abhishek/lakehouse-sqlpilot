/**
 * HTTPS Security Enforcement
 * Ensures all production traffic uses HTTPS
 */

export const enforceHttps = () => {
  // Only enforce in production
  if (import.meta.env.PROD) {
    // Check if we're on HTTP in production
    if (window.location.protocol === 'http:') {
      // Get the FORCE_HTTPS setting
      const forceHttps = import.meta.env.VITE_FORCE_HTTPS !== 'false'
      
      if (forceHttps) {
        // Redirect to HTTPS
        const httpsUrl = window.location.href.replace('http://', 'https://')
        console.warn('Redirecting to HTTPS:', httpsUrl)
        window.location.href = httpsUrl
      }
    }
    
    // Set security headers via meta tags
    addSecurityHeaders()
  }
}

const addSecurityHeaders = () => {
  // Content Security Policy - only allow HTTPS resources in production
  const csp = document.createElement('meta')
  csp.httpEquiv = 'Content-Security-Policy'
  csp.content = "upgrade-insecure-requests; default-src 'self' https:; script-src 'self' 'unsafe-inline' 'unsafe-eval' https:; style-src 'self' 'unsafe-inline' https:; img-src 'self' data: https:; font-src 'self' data: https:; connect-src 'self' https:;"
  document.head.appendChild(csp)
  
  // Strict Transport Security
  const hsts = document.createElement('meta')
  hsts.httpEquiv = 'Strict-Transport-Security'
  hsts.content = 'max-age=31536000; includeSubDomains'
  document.head.appendChild(hsts)
}

export const getSecureApiUrl = (url: string): string => {
  // In production, ensure API calls use HTTPS
  if (import.meta.env.PROD && url.startsWith('http://')) {
    return url.replace('http://', 'https://')
  }
  return url
}

export const validateSecureConnection = (): {
  isSecure: boolean
  warnings: string[]
} => {
  const warnings: string[] = []
  const isSecure = window.location.protocol === 'https:' || !import.meta.env.PROD
  
  if (!isSecure && import.meta.env.PROD) {
    warnings.push('Production environment should use HTTPS')
  }
  
  // Check for mixed content
  if (window.location.protocol === 'https:') {
    const apiUrl = import.meta.env.VITE_API_URL
    if (apiUrl && apiUrl.startsWith('http://')) {
      warnings.push('API URL uses HTTP while frontend uses HTTPS (mixed content)')
    }
  }
  
  return { isSecure, warnings }
}


